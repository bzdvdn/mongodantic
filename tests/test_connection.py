import os

from mongodantic import connect
from mongodantic.connection import _DBConnection, _connection_settings
from mongodantic.connection import DEFAULT_CONNECTION_NAME
from pymongo import MongoClient


class TestWriteConnectionParams:
    def setup(self):
        connect("mongodb://127.0.0.1:27017", "test")
        self.connection = _DBConnection(str(os.getpid()))

    def test_connection_params(self):
        db_name = _connection_settings[DEFAULT_CONNECTION_NAME].get("dbname")
        conn_string = _connection_settings[DEFAULT_CONNECTION_NAME].get(
            "connection_str"
        )
        assert db_name == "test"
        assert conn_string == "mongodb://127.0.0.1:27017"

    def test_connection(self):
        assert isinstance(self.connection._mongo_connection, MongoClient)

    def test_conection_database(self):

        assert self.connection._mongo_connection.get_database('test') == MongoClient(
            "mongodb://127.0.0.1:27017"
        ).get_database("test")
