import unittest

from bson import ObjectId

from mongodantic import init_db_connection_params, querybuilder
from mongodantic.models import MongoModel
from mongodantic.session import Session


class TestBasicOperation(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")

        class Ticket(MongoModel):
            name: str
            position: int
            config: dict
            sign: int = 1
            type_: str = 'ga'
            array: list = [1, 2]

            class Config:
                excluded_query_fields = ('sign', 'type')

        Ticket.querybuilder.drop_collection(force=True)
        self.Ticket = Ticket

    def test_collection_name(self):
        assert self.Ticket.collection_name == 'ticket'

    def test_insert_one(self):
        data = {
            'name': 'first',
            'position': 1,
            'config': {'param1': 'value'},
            'array': ['test', 'adv'],
        }
        object_id = self.Ticket.querybuilder.insert_one(**data)
        assert isinstance(object_id, ObjectId)

    def test_serialize(self):
        self.test_insert_one()
        data = self.Ticket.querybuilder.find_one().serialize(
            ('position', 'type_', 'sign')
        )
        keys = list(data.keys())
        assert 1 == keys.index('type_')
        assert 0 == keys.index('position')
        assert 2 == keys.index('sign')

    def test_insert_many(self):
        data = [
            self.Ticket(
                name='second',
                position=2,
                config={'param1': '2222'},
                array=['test', 'google'],
            ),
            self.Ticket(
                name='second',
                position=2,
                config={'param1': '3333'},
                array=['test', 'adv'],
            ),
        ]
        inserted = self.Ticket.querybuilder.insert_many(data)
        assert inserted == 2

    def test_insert_many_with_dict(self):
        data = [
            self.Ticket(
                name='third',
                position=3,
                config={'param1': '2222'},
                array=['test', 'google'],
            ).data,
            self.Ticket(
                name='four',
                position=4,
                config={'param1': '3333'},
                array=['test', 'adv'],
            ).data,
        ]
        inserted = self.Ticket.querybuilder.insert_many(data)
        assert inserted == 2

    def test_find_in_array(self):
        self.test_insert_many()
        data = self.Ticket.querybuilder.find_one(array__in=['google']).data
        assert data['array'] == ['test', 'google']
        miss = self.Ticket.querybuilder.find_one(array__in=['miss_data'])
        assert miss is None

    def test_find_with_regex(self):
        self.test_insert_many()
        data = self.Ticket.querybuilder.find_one(name__iregex="seCoNd")
        assert data.name == 'second'

    def test_count(self):
        self.test_insert_many()
        count = self.Ticket.querybuilder.count()
        assert count == 2

    def test_find_one(self):
        self.test_insert_one()
        data = self.Ticket.querybuilder.find_one(name='first')
        second = self.Ticket.querybuilder.find_one(_id=data._id)
        assert isinstance(data, MongoModel)
        assert data.name == 'first'
        assert data.position == 1
        assert second._id == data._id

    def test_find(self):
        self.test_insert_many()
        data = self.Ticket.querybuilder.find(name='second').list
        sort = self.Ticket.querybuilder.find(
            name='second', sort=-1, sort_fields=('_id',)
        ).first()
        assert sort.config == {'param1': '3333'}
        assert isinstance(data, list)
        assert len(data) == 2
        assert isinstance(data[0], MongoModel)

    def test_distinct(self):
        self.test_insert_many()
        data = self.Ticket.querybuilder.distinct('config.param1', name='second')
        assert data == ['2222', '3333']

    def test_queryset_serialize(self):
        self.test_insert_many()
        data = self.Ticket.querybuilder.find(name='second').serialize(
            fields=['name', 'config']
        )
        assert len(data[0]) == 2
        assert data[0]['config'] == {'param1': '2222'}
        assert data[0]['name'] == 'second'
        assert isinstance(data, list)

    def test_delete_one(self):
        self.test_insert_one()
        deleted = self.Ticket.querybuilder.delete_one(position=1)
        assert deleted == 1

    def test_delete_many(self):
        self.test_insert_many()
        deleted = self.Ticket.querybuilder.delete_many(position=2)
        assert deleted == 2

    def test_update_one(self):
        self.test_insert_one()
        data = self.Ticket.querybuilder.update_one(
            name='first', config__set={'updated': 1}
        )
        updated = self.Ticket.querybuilder.find_one(name='first')
        assert data == 1
        assert updated.config == {'updated': 1}

    def test_update_many(self):
        self.test_insert_many()
        data = self.Ticket.querybuilder.update_many(
            name='second', config__set={'updated': 3}
        )
        updated = self.Ticket.querybuilder.find_one(name='second')
        assert data == 2
        assert updated.config == {'updated': 3}

    def test_find_and_update(self):
        self.test_insert_one()
        data_default = self.Ticket.querybuilder.find_one_and_update(
            name='first', position__set=23
        )
        assert data_default.position == 23

        data_with_prejection = self.Ticket.querybuilder.find_one_and_update(
            name='first', position__set=12, projection_fields=['position']
        )
        assert isinstance(data_with_prejection, dict)
        assert data_with_prejection['position'] == 12

    def test_save_method(self):
        ticket = self.Ticket(name='save', position=123, config={})
        ticket = ticket.save()
        assert ticket.name == 'save'

    def test_delete_method(self):
        self.test_insert_one()
        ticket = self.Ticket.querybuilder.find_one(name='first')
        ticket.delete()
        data = self.Ticket.querybuilder.find_one(name='first')
        assert data is None

    def test_session(self):
        self.test_insert_one()
        with Session() as session:
            result = self.Ticket.querybuilder.find_one(name='first', session=session)
        assert result.name == 'first'

    # def test_session_with_transaction(self):

    #     with Session() as session:
    #         with session.start_transaction():
    #             result = self.Ticket.querybuilder.insert_one(
    #                 name='last', position=33333, config={}, session=session
    #             )
    #     assert result.name == 'first'
