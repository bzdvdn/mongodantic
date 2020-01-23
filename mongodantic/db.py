import os
from typing import Optional
from pymongo import MongoClient


class DBConnection(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DBConnection, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.connection_string = os.environ.get('MONGODANTIC_CONNECTION_STR')
        self.db_name = os.environ.get('MONGODANTIC_DBNAME')
        self.max_pool_size = int(os.environ.get('MONGODANTIC_POOL_SIZE', 100))
        self.ssl = True if int(os.environ.get('MONGODANTIC_SSL')) else False
        self.ssl_cert_path = os.environ.get('MONGODANTIC_ssl_cert_path')
        self._mongo_connection = self.__init_mongo_connection()
        self.database = self._mongo_connection.get_database(self.db_name)

    def __init_mongo_connection(self) -> MongoClient:
        if self.ssl:
            return MongoClient(
                self.connection_string,
                connect=True,
                ssl_ca_certs=self.ssl_cert_path,
                serverSelectionTimeoutMS=50000,
                maxPoolSize=self.max_pool_size,
                connectTimeoutMS=50000,
                socketTimeoutMS=50000,
                ssl_cert_reqs=self.ssl,
            )
        else:
            return MongoClient(
                self.connection_string,
                connect=True,
                serverSelectionTimeoutMS=50000,
                maxPoolSize=self.max_pool_size,
                connectTimeoutMS=50000,
                socketTimeoutMS=50000,
            )
