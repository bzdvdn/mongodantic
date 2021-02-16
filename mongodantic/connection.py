import os
from typing import Optional

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
    alias: str = str(os.getpid()),
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
