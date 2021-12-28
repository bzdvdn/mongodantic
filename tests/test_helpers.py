import re
import unittest
import pytest

from mongodantic.helpers import ExtraQueryMapper
from mongodantic.models import MongoModel
from mongodantic import connect


class TestExtraQueryMapper(unittest.TestCase):
    def setUp(self):
        connect("mongodb://127.0.0.1:27017", "test")

        class User(MongoModel):
            id: str
            name: str
            counter: int
            date: str

        User.querybuilder.drop_collection(force=True)
        self.User = User

    def test_in_extra_param(self):
        with pytest.raises(TypeError):
            ExtraQueryMapper(self.User, 'name').extra_query(['in'], (1, 3))
        extra = ExtraQueryMapper(self.User, 'name').extra_query(['in'], [1, 3])
        value = {'name': {'$in': ['1', '3']}}
        assert extra == value

    def test_nin_extra_param(self):
        with pytest.raises(TypeError):
            ExtraQueryMapper(self.User, 'name').extra_query(['nin'], (1, 3))
        extra = ExtraQueryMapper(self.User, 'name').extra_query(['nin'], [1, 3])
        value = {'name': {'$nin': ['1', '3']}}
        assert extra == value

    def test_ne_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(['ne'], 'test')
        value = {'name': {"$ne": 'test'}}
        assert extra == value

    def test_regex_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(['regex'], 'test')
        value = {'name': {'$regex': 'test'}}
        assert extra == value

    def test_regex_ne_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(['regex_ne'], 'test')
        value = {'name': {"$not": re.compile('test')}}
        assert extra == value

    def test_startswith_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(['startswith'], 'test')
        value = {'name': {'$regex': '^test'}}
        assert extra == value

    def test_endswith_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(['endswith'], 'test')
        value = {'name': {'$regex': 'test$'}}
        assert extra == value

    def test_not_endswith_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(
            ['not_endswith'], 'test'
        )
        value = {'name': {"$not": re.compile('test$')}}
        assert extra == value

    def test_not_startswith_extra_param(self):
        extra = ExtraQueryMapper(self.User, 'name').extra_query(
            ['not_startswith'], 'test'
        )
        value = {'name': {"$not": re.compile('^test')}}
        assert extra == value

    def test_range_extra_param(self):
        with pytest.raises(ValueError):
            ExtraQueryMapper(self.User, 'date').extra_query(['range'], 'test')
        extra = ExtraQueryMapper(self.User, 'date').extra_query(
            ['range'], ['2020-01-01', '2020-03-01']
        )
        value = {'date': {"$gte": '2020-01-01', "$lte": '2020-03-01'}}
        assert extra == value

    def test_lts_gts_params(self):
        extra = ExtraQueryMapper(self.User, 'date').extra_query(['lt'], '2020-01-01')
        assert extra == {'date': {"$lt": '2020-01-01'}}

        extra = ExtraQueryMapper(self.User, 'date').extra_query(['gt'], '2020-01-01')
        assert extra == {'date': {"$gt": '2020-01-01'}}

        extra = ExtraQueryMapper(self.User, 'date').extra_query(['gte'], '2020-01-01')
        assert extra == {'date': {"$gte": '2020-01-01'}}

        extra = ExtraQueryMapper(self.User, 'date').extra_query(['lte'], '2020-01-01')
        assert extra == {'date': {"$lte": '2020-01-01'}}

    def test_inc_params(self):
        with pytest.raises(ValueError):
            ExtraQueryMapper(self.User, 'counter').extra_query(['inc'], '2313123131')

        extra = ExtraQueryMapper(self.User, 'counter').extra_query(['inc'], 23)
        assert extra == {'$inc': {'counter': 23}}

    def test_exists_params(self):
        with pytest.raises(TypeError):
            ExtraQueryMapper(self.User, 'counter').extra_query(['exists'], '2313123131')

        extra = ExtraQueryMapper(self.User, 'counter').extra_query(['exists'], False)
        assert extra == {'counter': {'$exists': False}}
