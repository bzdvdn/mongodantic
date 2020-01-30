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

## Declare models
```python
from mongodantic.models import MongoModel

class Banner(MongoModel):
    banner_id: str
    name: str
    utm: dict
```

## Queries
```python
banner = Banner.find_one() # return a banner model obj
banner_data = Banner.find_one().data # return a dict
banners_generator = Banner.find() # return generator
banners_dict_generator = Banner.find().data
list_of_banners = Banner.find().list

# count
count = Banner.count(name='test')

# insert queries
Banner.insert_one(banner_id=1, name='test', utm={'utm_source': 'yandex', 'utm_campaign': 'cpc'})

banners = [Banner(banner_id=2, name='test2', utm={}), Banner(banner_id=3, name='test3', utm={})]
Banner.insert_many(banners) # list off models obj, or dicts

# update queries
Banner.update_one(banner_id=1, name__set='updated') # parameters that end __set - been updated  
Banner.update_many(name__set='update all names')

# delete queries
Banner.delete_one(banner_id=1) # delete one row
Banner.delete_many(banner_id=1) # delete many rows

# extra queries
Banner.find(banner_id__in=[1, 2]) # get data in list

Banner.find(banner_id__range=[1,10]) # get date from 1 to 10

Banner.find(name__regex='^test') # regex query

Banner.find(name__startswith='t') # startswith query

Banner.find(name__endswith='t') # endswith query

Banner.find(name__nin=[1, 2]) # not in list

Banner.find(name__ne='test') # != test

Banner.find(banner_id__gte=1, banner_id__lte=10) # id >=1 and id <=10
Banner.find(banner_id__gt=1, banner_id__lt=10) # id >1 and id <10

# bulk operations
from random import randint
banners = Banner.find()
to_update = []
for banner in banners:
    banner.banner_id = randint(1,100)
    to_update.append(banner)

Banner.bulk_update(banners, updated_fields=['banner_id'])

# bulk update or create

banners = [Banner(banner_id=23, name='new', utms={}), Banner(banner_id=1, name='test', utms={})]
Banner.bulk_update_or_create(banners, query_fields=['banner_id'])
```