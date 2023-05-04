import asyncio
from typing import Optional, List

from clients.db_client import DBClient
from clients.bronbro_api_client import BronbroApiClient
from common.constants import TOKENS_STARTED_FROM_U
from config.config import STAKED_DENOM


class AccountService:

    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()

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
            denom_to_search = item['denom']
            if item['denom'] not in TOKENS_STARTED_FROM_U and (item['denom'].startswith('u') or item['denom'].startswith('stu')):
                denom_to_search = item['denom'].replace('u', '', 1)
            if item['denom'] == 'basecro':
                denom_to_search = 'cro'
            exchange_rate = next((rate for rate in exchange_rates if rate.get('symbol').lower() == denom_to_search), None)
            item['price'] = exchange_rate.get('price') if exchange_rate else None
            item['exponent'] = exchange_rate.get('exponent') if exchange_rate else None
            item['symbol'] = exchange_rate.get('symbol') if exchange_rate else None
            item['logo'] = ''
        return balance

    def get_account_liquid_balance(self, address: str, exchange_rates: List[dict]) -> Optional[dict]:
        account_balance = self.db_client.get_account_balance(address)
        if account_balance and len(account_balance.coins):
            result = []
            for i in range(len(account_balance.coins.get('denom'))):
                result.append({
                    'denom': account_balance.coins.get('denom')[i],
                    'amount': account_balance.coins.get('amount')[i],
                })
            prettified_result = self.prettify_balance_structure(result)
            result_with_prices = self.add_additional_fields_to_balance(prettified_result, exchange_rates)
            result = {
                'native': [],
                'ibc': []
            }
            for balance_item in result_with_prices:
                if balance_item.get('denom') == STAKED_DENOM:
                    result['native'].append(balance_item)
                else:
                    result['ibc'].append(balance_item)
            return result
        else:
            return None

    def get_account_staked_balance(self, address: str, exchange_rates: List[dict]) -> Optional[List[dict]]:
        staked_balance = self.db_client.get_stacked_balance_for_address(address)
        if staked_balance:
            result = [{'denom': item.coin_denom, 'amount': item.sum_coin_amount_} for item in staked_balance]
            prettified_result = self.prettify_balance_structure(result)
            result_with_prices = self.add_additional_fields_to_balance(prettified_result, exchange_rates)
            return result_with_prices
        else:
            return None

    def get_account_unbonding_balance(self, address: str, exchange_rates: List[dict]) -> Optional[List[dict]]:
        unbonding_balance = self.db_client.get_unbonding_balance_for_address(address)
        if unbonding_balance:
            result = [{'denom': item.coin_denom, 'amount': item.sum_coin_amount_} for item in unbonding_balance]
            prettified_result = self.prettify_balance_structure(result)
            result_with_prices = self.add_additional_fields_to_balance(prettified_result, exchange_rates)
            return result_with_prices
        else:
            return None

    def get_account_rewards_balance(self, address: str, exchange_rates: List[dict]) -> Optional[List[dict]]:
        api_response = self.bronbro_api_client.get_address_rewards(address)
        if api_response and len(api_response.get('total', [])):
            result = api_response.get('total')
            for balance_item in result:
                balance_item['amount'] = float(balance_item['amount'])
            prettified_result = self.prettify_balance_structure(result)
            result_with_prices = self.add_additional_fields_to_balance(prettified_result, exchange_rates)
            return result_with_prices
        else:
            return None

    def get_account_balance(self, address: str) -> dict:
        exchange_rates = self.bronbro_api_client.get_exchange_rates()
        account_balance = {
            'liquid': self.get_account_liquid_balance(address, exchange_rates),
            'staked': self.get_account_staked_balance(address, exchange_rates),
            'unbonding': self.get_account_unbonding_balance(address, exchange_rates),
            'rewards': self.get_account_rewards_balance(address, exchange_rates),
        }
        return account_balance

    def get_annual_provision(self) -> int:
        response = self.bronbro_api_client.get_annual_provisions()
        return int(float(response.get('annual_provisions'))) if response else 0

    def get_community_tax(self) -> float:
        distribution_params = self.db_client.get_distribution_params()
        return distribution_params.params.get('community_tax') if distribution_params else 0

    def get_bonded_tokens_amount(self) -> int:
        staking_pool = self.db_client.get_staking_pool()
        return staking_pool.bonded_tokens if staking_pool else 0

    def get_account_info(self, address) -> dict:
        exchange_rates = self.bronbro_api_client.get_exchange_rates()
        account_staked_balance = self.get_account_staked_balance(address, exchange_rates)
        validators = self.get_validators(address)
        delegations_sum = next((balance.get('amount') for balance in account_staked_balance if balance.get('denom') == STAKED_DENOM), 0)
        annual_provision = self.get_annual_provision()
        community_tax = self.get_community_tax()
        bonded_tokens_amount = self.get_bonded_tokens_amount()
        apr = annual_provision * (1 - community_tax) / bonded_tokens_amount
        total_annual_provision = sum([x['coin']['amount'] * apr * (1 - x['commission']) for x in validators])
        return {
            "apr": apr,
            "voting_power": delegations_sum / bonded_tokens_amount,
            "rpde": total_annual_provision / 365.3,
            "staked": delegations_sum,
            "annual_provision": total_annual_provision
        }

    def get_validators(self, address):
        validators = self.db_client.get_validators(address)
        return [validator._asdict() for validator in validators]
