import os
from typing import Optional


def init_db_connection_params(connection_str: str, dbname: str, ssl: bool = False,
                              max_pool_size: int = 100, ssl_cert_path: Optional[str] = None) -> None:
    os.environ['MONGODANTIC_CONNECTION_STR'] = connection_str
    os.environ['MONGODANTIC_DBNAME'] = dbname
    os.environ['MONGODANTIC_SSL'] = '1' if ssl else '0'
    os.environ['MONGODANTICPOOL_SIZE'] = str(max_pool_size)
    if ssl_cert_path:
        os.environ['MONGODANTIC_SSL_CERT_PATH'] = ssl_cert_path
