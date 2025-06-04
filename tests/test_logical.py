from mongodantic.logical import Query
from mongodantic.models import MongoModel
from mongodantic.connection import connect
from mongodantic.querybuilder import QueryBuilder


class TestLogicalQuery:
    def setup_method(self):
        connect("mongodb://127.0.0.1:27017", "test")

        class Ticket(MongoModel):
            name: str
            position: int

            def __init__(self, **data):
                super().__init__(**data)
                if self.__class__.__querybuilder__ is None:
                    self.__class__.__querybuilder__ = QueryBuilder(
                        self.__class__)

        self.Ticket = Ticket
        # Initialize the model by creating an instance
        Ticket(name="init", position=0)
        Ticket.Q.drop_collection(force=True)

    def test_query_organization(self):
        query = (
            Query(name="123")
            | Query(name__ne="124") & Query(position=1)
            | Query(position=2)
        )
        data = query.to_query(self.Ticket)
        value = {
            '$or': [
                {'name': '123'},
                {'$and': [{'name': {'$ne': '124'}}, {'position': 1}]},
                {'position': 2},
            ]
        }
        assert data == value

    def test_logical_query_result(self):
        query = [
            self.Ticket(name='first', position=1),
            self.Ticket(name='second', position=2),
        ]
        inserted = self.Ticket.Q.insert_many(query)
        assert inserted == 2

        query = Query(name='first') | Query(position=1) & Query(name='second')
        data = self.Ticket.Q.find_one(query, sort=1)
        assert data.name == 'first'

        query = Query(position=3) | Query(position=1) & Query(name='second')
        data = self.Ticket.Q.find_one(query, sort=1)
        assert data is None

        query = Query(position=3) | Query(position=2) & Query(name='second')
        data = self.Ticket.Q.find_one(query, sort=1)
        assert data.name == 'second'
