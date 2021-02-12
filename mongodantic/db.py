from typing import Optional
from pymongo import MongoClient, database

all = ('_DBConnection', 'init_db_connection_params')

DEFAULT_CONNECTION_NAME = 'default'

_connection_settings = {}


def init_db_connection_params(
    connection_str: str,
    dbname: str,
    ssl: bool = False,
    max_pool_size: int = 100,
    ssl_cert_path: Optional[str] = None,
    server_selection_timeout_ms: int = 50000,
    connect_timeout_ms: int = 50000,
    socket_timeout_ms: int = 50000,
    alias: str = DEFAULT_CONNECTION_NAME,
) -> None:
    _connection_settings[alias] = {
        'connection_str': connection_str,
        'dbname': dbname,
        'ssl': ssl,
        'pool_size': max_pool_size,
        'server_selection_timeout_ms': server_selection_timeout_ms,
        'connect_timeout_ms': connect_timeout_ms,
        'socket_timeout_ms': socket_timeout_ms,
        'ssl_cert_path': ssl_cert_path,
    }


_connections: dict = {}


class _DBConnection(object):
    def __init__(self, alias: str = DEFAULT_CONNECTION_NAME):
        self._alias = alias
        self.connection_string = _connection_settings[alias]['connection_str']
        self.db_name = _connection_settings[alias]['dbname']
        self.max_pool_size = _connection_settings[alias]['pool_size']
        self.ssl = _connection_settings[alias]['ssl']
        self.ssl_cert_path = _connection_settings[alias]['ssl_cert_path']
        self.server_selection_timeout_ms = _connection_settings[alias][
            'server_selection_timeout_ms'
        ]
        self.connect_timeout_ms = _connection_settings[alias]['connect_timeout_ms']
        self.socket_timeout_ms = _connection_settings[alias]['socket_timeout_ms']
        self._mongo_connection = self._init_mongo_connection()
        self._database = None
        _connections[alias] = self

    def _init_mongo_connection(self, connect: bool = False) -> MongoClient:
        connection_params = dict(
            connect=connect,
            serverSelectionTimeoutMS=self.server_selection_timeout_ms,
            maxPoolSize=self.max_pool_size,
            connectTimeoutMS=self.connect_timeout_ms,
            socketTimeoutMS=self.socket_timeout_ms,
            retryWrites=False,
            retryReads=False,
        )
        if self.ssl:
            connection_params['tlsCAFile'] = self.ssl_cert_path
            connection_params['tlsAllowInvalidCertificates'] = self.ssl
        return MongoClient(self.connection_string, **connection_params)

    def _reconnect(self):
        del _connections[self.alias]
        return self.__init__(self.alias)

    def get_database(self) -> database.Database:
        if hasattr(self, '_database') and self._database:
            return self._database
        self._database = self._mongo_connection.get_database(self.db_name)
        return self._database

