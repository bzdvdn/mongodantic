import unittest
from bson import ObjectId

from mongodantic.models import MongoModel
from mongodantic.types import ObjectIdStr, ObjectId
from mongodantic import init_db_connection_params


class TestAggregation(unittest.TestCase):
    def setUp(self):
        init_db_connection_params("mongodb://127.0.0.1:27017", "test")

        class Product(MongoModel):
            title: str
            cost: float
            quantity: int

        class ProductImage(MongoModel):
            url: str
            product_id: ObjectIdStr

        Product.drop_collection(force=True)
        ProductImage.drop_collection(force=True)

        self.Product = Product
        self.ProductImage = ProductImage

    def test_aggregation_math_operation(self):
        data = [
            self.Product(title=str(i), cost=float(i), quantity=i) for i in range(1, 5)
        ]
        self.Product.insert_many(data)
        max_ = self.Product.aggregate_max(agg_field='cost')
        assert max_ == 4

        min_ = self.Product.aggregate_min(agg_field='cost')
        assert min_ == 1

        sum_ = self.Product.aggregate_sum(agg_field='cost')
        assert sum_ == 10

    def test_aggregate_lookup(self):

        product_inserted_id = self.Product.insert_one(
            title='product1', cost=23.00, quantity=23
        )
        print('pp - ', product_inserted_id)
        image_inserted_id = self.ProductImage.insert_one(
            url='http://localhost:8000/image.png',
            product_id=ObjectId(product_inserted_id),
        )

        product = self.Product.aggregate_lookup(
            local_field='_id',
            from_collection=self.ProductImage,
            foreign_field='product_id',
            title='product1',
        ).first()

        assert str(product.productimage[0].product_id) == str(product_inserted_id)

        image = self.ProductImage.aggregate_lookup(
            local_field='product_id',
            from_collection=self.Product,
            foreign_field='_id',
            with_unwing=True,
        ).first()
        assert str(image.product._id) == str(product_inserted_id)
