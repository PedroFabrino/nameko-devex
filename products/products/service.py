import logging

from marshmallow import ValidationError
from nameko.events import event_handler
from nameko.rpc import rpc
from nameko.web.handlers import http

from products import dependencies, schemas
from products.exceptions import NotFound



logger = logging.getLogger(__name__)


class ProductsService:

    name = 'products'

    storage = dependencies.Storage()

    @rpc
    def get(self, product_id):
        try:
            product = self.storage.get(product_id)
            if product is None:
                return 404, {"error": "Product not found"}
            return schemas.Product().dump(product).data
        except NotFound as e:
            logger.exception("The product: {product_id} does not exist")
            raise e
        except Exception as e:
            logger.exception("Error occurred while getting product")
            return e


    @rpc
    def list(self, page=1, page_size=10):
        try:
            # Perform pagination using SQLAlchemy's paginate function
            products = self.storage.list_paginated(page, page_size)
            
            return products
        except Exception as e:
            logger.exception("Error occurred while listing products")
            raise e

    @rpc
    def list_ids(self):
        try:
            products = self.storage.list()
            valid_product_ids = [product['id'] for product in products]
            return valid_product_ids
        except Exception as e:
            logger.exception("Error occurred while listing products")
            return set()

    @rpc
    def list_with_ids(self, product_ids):
        try:
            products = self.storage.filter_by_ids(product_ids)
            return schemas.Product(many=True).dump(products).data
        except Exception as e:
            logger.exception("Error occurred while listing products")
            return 500, {"error": "An error occurred while listing the products"}
    @rpc
    def create(self, product):
        try:
            product = schemas.Product(strict=True).load(product).data
            self.storage.create(product)
        except ValidationError as e:
            logger.exception("Validation error occurred while creating product")
            raise e
        except Exception as e:
            logger.exception("Error occurred while creating product")
            return 500, {"error": "An error occurred while creating the product"}
        
    @rpc
    def delete(self, product_id):
        try:
            self.storage.delete(product_id)
        except Exception as e:
            logger.exception("Error occurred while deleting product")
            return 500, {"error": "An error occurred while deleting the product: {product_id}"}

    @event_handler('orders', 'order_created')
    def handle_order_created(self, payload):
        try:
            for product in payload['order']['order_details']:
                self.storage.decrement_stock(
                    product['product_id'], product['quantity'])
        except Exception as e:
            logger.exception("Error occurred while handling order creation")
            return 500, {"error": "An error occurred while handling order creation"}
        