# mongodantic

##ettings
in your main file application
```python
from mongodantic import init_db_connection_params
connection_str = '<your connection url>'
db_name = '<name of database>'
# basic
init_db_connection_params(connection_str, db_name, max_pool_size=100)
# if u use ssl
init_db_connection_params(connection_str, db_name, max_pool_size=100, ssl=True, ssl_cert_path='<path to cert>')
# extra params
server_selection_timeout_ms = 50000 # pymongo serverSelectionTimeoutMS
connect_timeout_ms = 50000 # pymongo connectTimeoutMS
socket_timeout_ms = 50000 # pymongo socketTimeoutMS
```

## Declare models
```python
from mongodantic.models import MongoModel

class Banner(MongoModel):
    banner_id: str
    name: str
    utm: dict

# if you need take an existing collection, you must reimplement set_collection_name method like that
class Banner(MongoModel):
    ...

    @classmethod
    def set_collection_name(cls) -> str:
        return 'banner_test'
    


```

## Queries
```python
banner = Banner.querybuilder.find_one() # return a banner model obj
# skip and limit
banner_with_skip_and_limit = Banner.querybuilder.find(skip_rows=10, limit_rows=10)
banner_data = Banner.querybuilder.find_one().data # return a dict
banners_queryset= Banner.querybuilder.find() # return QuerySet object
banners_dict = Banner.querybuilder.find().data
list_of_banners = Banner.querybuilder.find().list
banners_generator = Banner.querybuilder.find().generator # generator of Banner objects
banners_generator_of_dicts = Banner.querybuilder.find().data_generator # generator of Banner objects
count, banners = Banner.querybuilder.find_with_count() # return tuple(int, QuerySet)

serializeble_fields = Banner.querybuilder.find().serialize(['utm', 'banner_id', 'name']) # return list with dict like {'utm':..., 'banner_id': ..,'name': ...}
generator_serializeble_fields = Banner.querybuilder.find().serialize_generator(['utm', 'banner_id', 'name']) # return generator
json_serializeble_fields = Banner.querybuilder.find().serialize_json(['utm', 'banner_id', 'name']) # returnn json str serializeble

# count
count = Banner.querybuilder.count(name='test')

# insert queries
Banner.querybuilder.insert_one(banner_id=1, name='test', utm={'utm_source': 'yandex', 'utm_medium': 'cpc'})

banners = [Banner(banner_id=2, name='test2', utm={}), Banner(banner_id=3, name='test3', utm={})]
Banner.querybuilder.insert_many(banners) # list off models obj, or dicts
Banner.querybuilder.bulk_create(banners, batch_size=1000) # insert_many with batch

# update queries
Banner.querybuilder.update_one(banner_id=1, name__set='updated') # parameters that end __set - been updated  
Banner.querybuilder.update_many(name__set='update all names')

# delete queries
Banner.querybuilder.delete_one(banner_id=1) # delete one row
Banner.querybuilder.delete_many(banner_id=1) # delete many rows

# extra queries
Banner.querybuilder.find(banner_id__in=[1, 2]) # get data in list

Banner.querybuilder.find(banner_id__range=[1,10]) # get date from 1 to 10

Banner.querybuilder.find(name__regex='^test') # regex query

Banner.querybuilder.find(name__startswith='t') # startswith query

Banner.querybuilder.find(name__endswith='t') # endswith query
Banner.querybuilder.find(name__not_startswith='t') # not startswith query

Banner.querybuilder.find(name__not_endswith='t') # not endswith query


Banner.querybuilder.find(name__nin=[1, 2]) # not in list

Banner.querybuilder.find(name__ne='test') # != test

Banner.querybuilder.find(banner_id__gte=1, banner_id__lte=10) # id >=1 and id <=10
Banner.querybuilder.find(banner_id__gt=1, banner_id__lt=10) # id >1 and id <10
Banner.querybuilder.find_one(banner_id=1, utm__utm_medium='cpm') # find banner where banner_id=1, and utm['utm_medium'] == 'cpm'

Banner.querybuilder.update_one(banner_id=1, utm__utm_source__set='google') # update utms['utm_source'] in Banner

# find and update
Banner.querybuilder.find_and_update(banner_id=1, name__set='updated', projection_fields=['name': True]) # return {'name': 'updated}
Banner.querybuilder.find_and_update(banner_id=1, name__set='updated') # return Banner obj


# find and replace
Banner.querybuilder.find_and_update(banner_id=1, Banner(banner_id=1, name='uptated'), projection={'name': True})
# return {'name': 'updated}
Banner.querybuilder.find_and_update(banner_id=1, Banner(banner_id=1, name='uptated')) # return Banner obj


# bulk operations
from random import randint
banners = Banner.querybuilder.find()
to_update = []
for banner in banners:
    banner.banner_id = randint(1,100)
    to_update.append(banner)

Banner.querybuilder.bulk_update(banners, updated_fields=['banner_id'])

# bulk update or create

banners = [Banner(banner_id=23, name='new', utms={}), Banner(banner_id=1, name='test', utms={})]
Banner.querybuilder.bulk_update_or_create(banners, query_fields=['banner_id'])

# aggregate with sum, min, max
class Stats(MongoModel):
    id: int
    cost: float
    clicks: int
    shows: int
    date: str

from mongodantic.aggregation import Sum, Min, Max

Stats.querybuilder.aggregate(date='2020-01-20', aggregation=Sum('cost'))
Stats.querybuilder.aggregate(date='2020-01-20', aggregation=Min('clicks'))
Stats.querybuilder.aggregate(date='2020-01-20', aggregation=Max('shows'))

# sessions
from mongodantic.session import Session
with Session() as session:
    Banner.querybuilder.find(skip_rows=1, limit_rows=1, session=session).data


# logical
from mongodantic.logical import Query
data = Banner.querybuilder.find_one(Query(name='test') | Query(name__regex='testerino'))

```