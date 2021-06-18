import os
from typing import Optional
from pymongo import MongoClient, database

__all__ = (
    'connect',
    'set_connection_env',
    'get_connection_env',
    '_DBConnection',
    '_get_connection',
)

DEFAULT_CONNECTION_NAME = 'default'
_connections: dict = {}
_connection_settings = {}


def connect(
    connection_str: str,
    dbname: str,
    ssl: bool = False,
    max_pool_size: int = 100,
    ssl_cert_path: Optional[str] = None,
    server_selection_timeout_ms: int = 50000,
    connect_timeout_ms: int = 50000,
    socket_timeout_ms: int = 50000,
    env_name: str = DEFAULT_CONNECTION_NAME,
) -> None:
    _connection_settings[env_name] = {
        'connection_str': connection_str,
        'dbname': dbname,
        'ssl': ssl,
        'pool_size': max_pool_size,
        'server_selection_timeout_ms': server_selection_timeout_ms,
        'connect_timeout_ms': connect_timeout_ms,
        'socket_timeout_ms': socket_timeout_ms,
        'ssl_cert_path': ssl_cert_path,
    }
    _get_connection(str(os.getpid()))


def set_connection_env(name: str = DEFAULT_CONNECTION_NAME):
    os.environ['MONGODANTIC_DB_ENV'] = name


def get_connection_env() -> str:
    return os.environ.get('MONGODANTIC_DB_ENV', DEFAULT_CONNECTION_NAME)


class _DBConnection(object):
    def __init__(
        self, alias: str = str(os.getpid()), env_name: str = DEFAULT_CONNECTION_NAME
    ):
        self._alias = alias
        self.connection_string = _connection_settings[env_name]['connection_str']
        self.db_name = _connection_settings[env_name]['dbname']
        self.max_pool_size = _connection_settings[env_name]['pool_size']
        self.ssl = _connection_settings[env_name]['ssl']
        self.ssl_cert_path = _connection_settings[env_name]['ssl_cert_path']
        self.server_selection_timeout_ms = _connection_settings[env_name][
            'server_selection_timeout_ms'
        ]
        self.connect_timeout_ms = _connection_settings[env_name]['connect_timeout_ms']
        self.socket_timeout_ms = _connection_settings[env_name]['socket_timeout_ms']
        self._mongo_connection = self._init_mongo_connection()
        self._database = None

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
        self.close()
        return self.__init__(self._alias)

    def get_database(self) -> database.Database:
        if hasattr(self, '_database') and self._database:
            return self._database
        self._database = self._mongo_connection.get_database(self.db_name)
        return self._database

    def close(self) -> None:
        old_connection = _connections.pop(self._alias, None)
        if old_connection:
            old_connection._mongo_connection.close()
            del old_connection

    def __del__(self):
        self.close()


def _get_connection(
    alias: str, env_name: str = DEFAULT_CONNECTION_NAME
) -> _DBConnection:
    connection = _connections.get(str(alias))
    if not connection:
        connection = _DBConnection(str(alias), env_name=env_name)
        _connections[alias] = connection
    return connection


# for old vervions
def init_db_connection_params(
    connection_str: str,
    dbname: str,
    ssl: bool = False,
    max_pool_size: int = 100,
    ssl_cert_path: Optional[str] = None,
    server_selection_timeout_ms: int = 50000,
    connect_timeout_ms: int = 50000,
    socket_timeout_ms: int = 50000,
    env_name: str = DEFAULT_CONNECTION_NAME,
):
    return connect(**locals())
