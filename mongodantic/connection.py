import os
from typing import Optional

__all__ = ('init_db_connection_params', 'set_connection_env', 'get_connection_env')

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


def set_connection_env(name: str = DEFAULT_CONNECTION_NAME):
    os.environ['MONGODANTIC_DB_ENV'] = name


def get_connection_env() -> str:
    return os.environ.get('MONGODANTIC_DB_ENV', DEFAULT_CONNECTION_NAME)

