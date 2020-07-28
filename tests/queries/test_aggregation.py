import unittest
from bson import ObjectId
from random import randint

from mongodantic.models import MongoModel
from mongodantic.types import ObjectIdStr, ObjectId
from mongodantic import init_db_connection_params
from mongodantic.lookup import Lookup

product_types = {1: 'phone', 2: 'book', 3: 'food'}


class TestAggregation(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")

        class Product(MongoModel):
            title: str
            cost: float
            quantity: int
            product_type: str

        class ProductImage(MongoModel):
            url: str
            product_id: ObjectIdStr

        Product.querybuilder.drop_collection(force=True)
        ProductImage.querybuilder.drop_collection(force=True)

        self.Product = Product
        self.ProductImage = ProductImage

    def test_aggregation_math_operation(self):
        data = [
            self.Product(
                title=str(i),
                cost=float(i),
                quantity=i,
                product_type=product_types[randint(1, 3)],
            )
            for i in range(1, 5)
        ]
        self.Product.querybuilder.insert_many(data)
        max_ = self.Product.querybuilder.aggregate_max(agg_field='cost')
        assert max_ == 4

        min_ = self.Product.querybuilder.aggregate_min(agg_field='cost')
        assert min_ == 1

        sum_ = self.Product.querybuilder.aggregate_sum(agg_field='cost')
        assert sum_ == 10

    def test_aggregation_multiply(self):
        data = [
            self.Product(
                title=str(i),
                cost=float(i),
                quantity=i - 1,
                product_type=product_types[2],
            )
            for i in range(1, 5)
        ]
        self.Product.querybuilder.insert_many(data)
        result_sum = self.Product.querybuilder.aggregate_sum_multiply(
            agg_fields=['cost', 'quantity']
        )
        assert result_sum == {'cost__sum': 10.0, 'quantity__sum': 6}

        result_max = self.Product.querybuilder.aggregate_max_multiply(
            agg_fields=['cost', 'quantity']
        )
        assert result_max == {'cost__max': 4.0, 'quantity__max': 3}

        result_min = self.Product.querybuilder.aggregate_min_multiply(
            agg_fields=['cost', 'quantity']
        )
        assert result_min == {'cost__min': 1.0, 'quantity__min': 0}

        result_multiply = self.Product.querybuilder.aggregate_multiply_math_operations(
            agg_fields=['cost', 'quantity'],
            fields_operations={'cost': 'sum', 'quantity': 'max'},
        )
        assert result_multiply == {'cost__sum': 10.0, 'quantity__max': 3}

        result_count = self.Product.querybuilder.aggregate_count(
            agg_field='product_type'
        )
        assert result_count == {'book': 4}

        result_multiply_count = self.Product.querybuilder.aggregate_multiply_count(
            agg_fields=['product_type', 'quantity']
        )
        assert result_multiply_count == [
            {'_id': {'product_type': 'book', 'quantity': 3}, 'count': 1},
            {'_id': {'product_type': 'book', 'quantity': 2}, 'count': 1},
            {'_id': {'product_type': 'book', 'quantity': 0}, 'count': 1},
            {'_id': {'product_type': 'book', 'quantity': 1}, 'count': 1},
        ]

    def test_aggregate_lookup(self):

        product_inserted_id = self.Product.querybuilder.insert_one(
            title='product1',
            cost=23.00,
            quantity=23,
            product_type=product_types[randint(1, 3)],
        )
        image_inserted_id = self.ProductImage.querybuilder.insert_one(
            url='http://localhost:8000/image.png',
            product_id=ObjectId(product_inserted_id),
        )
        product = self.Product.querybuilder.aggregate_lookup(
            lookup=Lookup(
                self.ProductImage, local_field='_id', foreign_field='product_id',
            ),
            title='product1',
        ).first()

        assert str(product.productimage[0].product_id) == str(product_inserted_id)

        image = self.ProductImage.querybuilder.aggregate_lookup(
            lookup=Lookup(
                local_field='product_id',
                from_collection=self.Product,
                foreign_field='_id',
                with_unwind=True,
            )
        ).first()
        assert str(image.product._id) == str(product_inserted_id)
