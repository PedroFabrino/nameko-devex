from nameko import config
from nameko.extensions import DependencyProvider
import redis

from products.exceptions import NotFound


REDIS_URI_KEY = 'REDIS_URI'


class StorageWrapper:
    """
    Product storage

    A very simple example of a custom Nameko dependency. Simplified
    implementation of products database based on Redis key value store.
    Handling the product ID increments or keeping sorted sets of product
    names for ordering the products is out of the scope of this example.

    """

    NotFound = NotFound

    def __init__(self, client):
        self.client = client

    def _format_key(self, product_id):
        return 'products:{}'.format(product_id)

    def _from_hash(self, document):
        return {
            'id': document[b'id'].decode('utf-8'),
            'title': document[b'title'].decode('utf-8'),
            'passenger_capacity': int(document[b'passenger_capacity']),
            'maximum_speed': int(document[b'maximum_speed']),
            'in_stock': int(document[b'in_stock'])
        }
    
    def _from_hash_decoded(self, document):
        return {
            'id': document['id'].decode('utf-8'),
            'title': document['title'].decode('utf-8'),
            'passenger_capacity': int(document['passenger_capacity']) if document.get('passenger_capacity') else None,
            'maximum_speed': int(document['maximum_speed']) if document.get('maximum_speed') else None,
            'in_stock': int(document['in_stock']) if document.get('in_stock') else None
        }
    
    def _get_paginated_keys(self, page, page_size):
        # Retrieve all product keys
        keys = self.client.keys(self._format_key('*'))
        # Calculate the start and end indices based on the pagination parameters
        start = (page - 1) * page_size
        end = start + page_size
        # Return the paginated keys
        return keys[start:end]

    def list_paginated(self, page=1, page_size=10):
        keys = self._get_paginated_keys(page, page_size)
        products = []
        for key in keys:
            products.append(self._from_hash(self.client.hgetall(key)))
        return products

    def get(self, product_id):
        product = self.client.hgetall(self._format_key(product_id))
        if not product:
            raise NotFound('Product ID {} does not exist'.format(product_id))
        else:
            return self._from_hash(product)

    def filter_by_ids(self, product_ids):
        for product_id in product_ids:
            try:
                product = self.get(product_id)
                yield product
            except NotFound:
                continue

    def list(self):
        keys = self.client.keys(self._format_key('*'))
        for key in keys:
            yield self._from_hash(self.client.hgetall(key))

    def create(self, product):
        self.client.hmset(
            self._format_key(product['id']),
            product)

    def decrement_stock(self, product_id, amount):
        return self.client.hincrby(
            self._format_key(product_id), 'in_stock', -amount)

    def delete(self, product_id):
        self.client.delete(self._format_key(product_id))


class Storage(DependencyProvider):

    def setup(self):
        self.client = redis.StrictRedis.from_url(config.get(REDIS_URI_KEY))

    def get_dependency(self, worker_ctx):
        return StorageWrapper(self.client)
