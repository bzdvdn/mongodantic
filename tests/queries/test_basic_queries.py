import unittest
from bson import ObjectId

from mongodantic.models import MongoModel
from mongodantic import init_db_connection_params



class TestBasicQueries(unittest.TestCase):
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
            self.Ticket(name='second', position=2, config={'param1': '2222'})
        ]
        inserted = self.Ticket.insert_many(data)
        assert inserted == 2