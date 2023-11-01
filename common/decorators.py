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

    def wrapper(*args, **kwargs):
        group_by = detailing_mapper(args[3])
        from_height = args[0].db_client.get_min_date_height(args[1]).height
        to_block_height = args[0].db_client.get_max_date_height(args[2])
        if not to_block_height:
            to_block_height = args[0].db_client.get_latest_height()
        to_height = to_block_height.height
        new_args = list(args)
        new_args[3] = group_by
        new_args.append(from_height)
        new_args.append(to_height)
        result = func(*tuple(new_args), **kwargs)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    return wrapper


def history_statistics_handler_for_view(func):

    def detailing_mapper(detailing):
        mapper = {
            'hour': 'toStartOfHour',
            'day': 'DATE',
            'week': 'toStartOfWeek',
            'month': 'toStartOfMonth'
        }
        return mapper.get(detailing, 'DATE')

    def wrapper(*args, **kwargs):
        group_by = detailing_mapper(args[3])
        new_args = list(args)
        new_args[3] = group_by
        result = func(*tuple(new_args), **kwargs)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    return wrapper
