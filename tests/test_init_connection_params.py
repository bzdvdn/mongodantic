import unittest
import os
from mongodantic import init_db_connection_params
from mongodantic.db import DBConnection
from pymongo import MongoClient


class TestWriteConnectionParams(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test_db")
        self.connection = DBConnection()

    def test_connection_params(self):
        db_name = os.environ.get("MONGODANTIC_DBNAME")
        conn_string = os.environ.get("MONGODANTIC_CONNECTION_STR")
        self.assertEqual(db_name, "test_db")
        self.assertEqual(conn_string, "mongodb://127.0.0.1:27017")

    def test_connection(self):
        assert isinstance(self.connection._mongo_connection, MongoClient)

    # def test_conection_database(self):
    #     assert self.connection.databas
