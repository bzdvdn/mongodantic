# mongodantic

## Settings
in your main file application
```python
from mongodantic import init_db_connection_params
connection_str = '<your connection url>'
db_name = '<name of database>'
# basic
init_db_connection_params(connection_str, db_name, max_pool_size=100)
# if u use ssl
init_db_connection_params(connection_str, db_name, max_pool_size=100, ssl=True, ssl_cert_path='<path to cert>')
```
