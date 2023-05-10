import asyncio
from typing import List

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from common.constants import TOKENS_STARTED_FROM_U


class BalancePrettifierService:
    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()

    def get_denom_to_search_in_api(self, denom):
        denom_to_search = denom
        if denom not in TOKENS_STARTED_FROM_U and (
                denom.startswith('u') or denom.startswith('stu')):
            denom_to_search = denom.replace('u', '', 1)
        if denom == 'basecro':
            denom_to_search = 'cro'
        return denom_to_search

    def prettify_balance_structure(self, balance: List[dict]) -> List[dict]:
        denoms_to_prettify = [item['denom'] for item in balance if item['denom'].startswith('ibc/')]
        mapped_denoms = asyncio.run(self.bronbro_api_client.get_symbols_from_denoms(denoms_to_prettify))
        for item in balance:
            if item['denom'].startswith('ibc/'):
                prettified_denom = next((mapped_denom['symbol'] for mapped_denom in mapped_denoms if mapped_denom['denom'] == item['denom']), item['denom'])
                item['denom'] = prettified_denom
        return balance

    def add_additional_fields_to_balance(self, balance: List[dict], exchange_rates: List[dict]) -> List[dict]:
        for item in balance:
            denom_to_search = self.get_denom_to_search_in_api(item['denom'])
            exchange_rate = next((rate for rate in exchange_rates if rate.get('symbol').lower() == denom_to_search), None)
            item['price'] = exchange_rate.get('price') if exchange_rate else None
            item['exponent'] = exchange_rate.get('exponent') if exchange_rate else None
            item['symbol'] = exchange_rate.get('symbol') if exchange_rate else None
        return balance

    def add_logo_to_balance_items(self, balance: List['dict']) -> List[dict]:
        symbols = [self.get_denom_to_search_in_api(item['denom']) for item in balance]
        symbols_with_logos = asyncio.run(self.bronbro_api_client.get_symbols_logos(symbols))
        for item in balance:
            item_logo = next((symbol_with_logo.get('logo') for symbol_with_logo in symbols_with_logos if symbol_with_logo.get('symbol') == self.get_denom_to_search_in_api(item['denom'])), '')
            item['logo'] = item_logo
        return balance