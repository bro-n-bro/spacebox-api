from collections import namedtuple


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


# def convert_tuples_to_named_tuples_in_query_result(func):
#     def wrapper(*args, **kwargs):
#         list_of_items, field_names = func(*args, **kwargs)
#         Record = namedtuple("Record", field_names)
#         result = [Record(*item for item in list_of_items)]
#         return result
#     return wrapper
