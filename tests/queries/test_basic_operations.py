import unittest
from bson import ObjectId

from mongodantic.models import MongoModel
from mongodantic import init_db_connection_params


class TestBasicOperation(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")

        class Ticket(MongoModel):
            name: str
            position: int
            config: dict

        Ticket.drop_collection(force=True)
        self.Ticket = Ticket

    def test_insert_one(self):
        data = {'name': 'first', 'position': 1, 'config': {'param1': 'value'}}
        object_id = self.Ticket.insert_one(**data)
        assert isinstance(object_id, ObjectId)

    def test_insert_many(self):
        data = [
            self.Ticket(name='second', position=2, config={'param1': '2222'}),
            self.Ticket(name='second', position=2, config={'param1': '3333'}),
        ]
        inserted = self.Ticket.insert_many(data)
        assert inserted == 2

    def test_find_one(self):
        self.test_insert_one()
        data = self.Ticket.find_one(name='first')
        assert isinstance(data, MongoModel)
        assert data.name == 'first'
        assert data.position == 1

    def test_find(self):
        self.test_insert_many()
        data = self.Ticket.find(name='second').list
        sort = self.Ticket.find(name='second', sort=-1).first()
        assert sort.config == {'param1': '3333'}
        assert isinstance(data, list)
        assert len(data) == 2
        assert isinstance(data[0], MongoModel)

    def test_delete_one(self):
        self.test_insert_one()
        deleted = self.Ticket.delete_one(position=1)
        assert deleted == 1

    def test_delete_many(self):
        self.test_insert_many()
        deleted = self.Ticket.delete_many(position=2)
        assert deleted == 2

    def test_update_one(self):
        self.test_insert_one()
        data = self.Ticket.update_one(name='first', config__set={'updated': 1})
        updated = self.Ticket.find_one(name='first')
        assert data == 1
        assert updated.config == {'updated': 1}

    def test_update_many(self):
        self.test_insert_many()
        data = self.Ticket.update_many(name='second', config__set={'updated': 3})
        updated = self.Ticket.find_one(name='second')
        assert data == 2
        assert updated.config == {'updated': 3}

    def test_find_and_update(self):
        self.test_insert_one()
        data_default = self.Ticket.find_one_and_update(name='first', position__set=23)
        assert data_default.position == 23

        data_with_prejection = self.Ticket.find_one_and_update(
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
        ticket = self.Ticket.find_one(name='first')
        ticket.delete()
        data = self.Ticket.find_one(name='first')
        assert data is None
