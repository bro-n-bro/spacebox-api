import json
from collections import namedtuple

from config.config import NETWORK


def response_decorator(func):
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        if 200 <= response.status_code < 300:
            return response.json()
        else:
            return None
    return wrapper


def get_first_if_exists(func):
    def wrapper(*args, **kwargs):
        list_of_items = func(*args, **kwargs)
        if len(list_of_items):
            return list_of_items[0]
        else:
            return None
    return wrapper


def add_address_to_response(func):
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        data = response.json
        data['address'] = kwargs.get('address')
        response.data = json.dumps(data)
        return response

    wrapper.__name__ = func.__name__
    return wrapper
