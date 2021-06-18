import unittest
import os
from mongodantic import init_db_connection_params
from mongodantic.connection import _DBConnection, _connection_settings
from mongodantic.connection import DEFAULT_CONNECTION_NAME
from pymongo import MongoClient


class TestWriteConnectionParams(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")
        self.connection = _DBConnection(str(os.getpid()))

    def test_connection_params(self):
        db_name = _connection_settings[DEFAULT_CONNECTION_NAME].get("dbname")
        conn_string = _connection_settings[DEFAULT_CONNECTION_NAME].get(
            "connection_str"
        )
        self.assertEqual(db_name, "test")
        self.assertEqual(conn_string, "mongodb://127.0.0.1:27017")

    def test_connection(self):
        assert isinstance(self.connection._mongo_connection, MongoClient)

    def test_conection_database(self):
        self.assertEqual(
            self.connection._mongo_connection.get_database('test'),
            MongoClient("mongodb://127.0.0.1:27017").get_database("test"),
        )
