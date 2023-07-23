import json
from collections import namedtuple
from datetime import datetime, timedelta

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


def history_statistics_handler(func):

    def detailing_mapper(detailing):
        mapper = {
            'hour': 'toStartOfHour',
            'day': 'DATE',
            'week': 'toStartOfWeek',
            'month': 'toStartOfMonth'
        }
        return mapper.get(detailing, 'DATE')

    def wrapper(_self, from_date, to_date, detailing):
        to_date = str((datetime.strptime(to_date, "%Y-%m-%d").date() + timedelta(days=1)))
        group_by = detailing_mapper(detailing)
        result = func(_self, from_date, to_date, group_by)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    return wrapper