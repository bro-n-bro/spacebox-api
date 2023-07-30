import asyncio
from typing import List

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from common.constants import TOKENS_STARTED_FROM_U
from common.in_memory_cache import set_cached_denoms, CACHED_SYMBOLS, CACHED_LOGOS, set_cached_logos, \
    get_denom_to_search_in_api


class BalancePrettifierService:
    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()
        self.exchange_rates = self.set_exchange_rates()

    def set_exchange_rates(self):
        result = {}
        exchange_rates = self.bronbro_api_client.get_exchange_rates()
        for exchange_rate in exchange_rates:
            result[exchange_rate.get('symbol').lower()] = {
                'price': exchange_rate.get('price'),
                'exponent': exchange_rate.get('exponent'),
                'symbol': exchange_rate.get('symbol'),
            }
        return result

    def prettify_balance_structure(self, balance: List[dict]) -> List[dict]:
        denoms_to_prettify = [item['denom'] for item in balance if item['denom'].startswith('ibc/') and item['denom'] not in CACHED_SYMBOLS]
        mapped_denoms = asyncio.run(self.bronbro_api_client.get_symbols_from_denoms(denoms_to_prettify))
        set_cached_denoms(mapped_denoms)
        for item in balance:
            if item['denom'].startswith('ibc/'):
                item['denom'] = CACHED_SYMBOLS[item['denom']]
        return balance

    def add_additional_fields_to_balance_item(self, balance_item: dict) -> dict:
        denom_to_search = get_denom_to_search_in_api(balance_item['denom'])
        exchange_rate = self.exchange_rates.get(denom_to_search, None)
        balance_item['price'] = exchange_rate.get('price') if exchange_rate else 0
        balance_item['exponent'] = exchange_rate.get('exponent') if exchange_rate else 0
        balance_item['symbol'] = exchange_rate.get('symbol') if exchange_rate else balance_item['denom']
        return balance_item

    def add_additional_fields_to_balance(self, balance: List[dict]) -> List[dict]:
        for item in balance:
            self.add_additional_fields_to_balance_item(item)
        return balance

    def add_logo_to_balance_items(self, balance: List['dict']) -> List[dict]:
        symbols = []
        for balance_item in balance:
            denom_to_search = get_denom_to_search_in_api(balance_item['denom'])
            if not CACHED_LOGOS.get(denom_to_search):
                symbols.append(denom_to_search)
        symbols_with_logos = asyncio.run(self.bronbro_api_client.get_symbols_logos(symbols))
        set_cached_logos(symbols_with_logos)
        for item in balance:
            item['logo'] = CACHED_LOGOS.get(get_denom_to_search_in_api(item['denom']), '')
        return balance

    def get_and_build_token_info(self, token):
        token_info = {
            'denom': token,
        }
        token_info = self.add_additional_fields_to_balance_item(token_info)
        token_to_get_logo = get_denom_to_search_in_api(token_info['denom'])
        logo_response = self.bronbro_api_client.get_token_logo(token_to_get_logo)
        token_info['logo'] = logo_response.get('logo_URIs', {}).get('svg', '') if logo_response else ''
        return token_info

    def clean_cache_after_request(self):
        self.exchange_rates = {}
