import asyncio
import datetime
import json
from typing import Optional, List
import copy

import dateutil.parser

from clients.db_client import DBClient
from clients.bronbro_api_client import BronbroApiClient
from common.constants import TOKENS_STARTED_FROM_U
from config.config import STAKED_DENOM, MINTSCAN_AVATAR_URL
from services.balance_prettifier import BalancePrettifierService


class AccountService:

    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()
        self.balance_prettifier_service = BalancePrettifierService()

    def get_account_liquid_balance(self, address: str) -> Optional[dict]:
        account_balance = self.db_client.get_account_balance(address)
        if account_balance and len(account_balance.coins):
            result = []
            for i in range(len(account_balance.coins.get('denom'))):
                result.append({
                    'denom': account_balance.coins.get('denom')[i],
                    'amount': account_balance.coins.get('amount')[i],
                })
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(result)
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            result = {
                'native': [],
                'ibc': []
            }
            for balance_item in result_with_logos:
                if balance_item.get('denom') == STAKED_DENOM:
                    result['native'].append(balance_item)
                else:
                    result['ibc'].append(balance_item)
            return result
        else:
            return None

    def get_account_staked_balance(self, address: str) -> Optional[List[dict]]:
        staked_balance = self.db_client.get_stacked_balance_for_address(address)
        if staked_balance:
            result = [{'denom': item.denom, 'amount': item.amount} for item in staked_balance]
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(result)
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            return result_with_logos
        else:
            return None

    def get_account_unbonding_balance(self, address: str) -> Optional[List[dict]]:
        unbonding_balance = self.db_client.get_unbonding_balance_for_address(address)
        if unbonding_balance:
            result = [{'denom': item.coin_denom, 'amount': item.sum_coin_amount_} for item in unbonding_balance]
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(result)
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            return result_with_logos
        else:
            return None

    def get_account_rewards_balance(self, address: str) -> Optional[List[dict]]:
        api_response = self.bronbro_api_client.get_address_rewards(address)
        if api_response and len(api_response.get('total', [])):
            result = api_response.get('total')
            for balance_item in result:
                balance_item['amount'] = float(balance_item['amount'])
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(result)
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            return result_with_logos
        else:
            return None

    def get_account_balance(self, address: str) -> dict:
        account_balance = {
            'liquid': self.get_account_liquid_balance(address),
            'staked': self.get_account_staked_balance(address),
            'unbonding': self.get_account_unbonding_balance(address),
            'rewards': self.get_account_rewards_balance(address),
        }
        return account_balance

    def serialize_liquid_balance(self, liquid_response):
        if liquid_response and len(liquid_response.get('balances', [])):
            balance_items = liquid_response.get('balances')
            for balance_item in balance_items:
                balance_item['amount'] = float(balance_item['amount'])
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(balance_items)
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            result = {
                'native': [],
                'ibc': []
            }
            for balance_item in result_with_logos:
                if balance_item.get('denom') == STAKED_DENOM:
                    result['native'].append(balance_item)
                else:
                    result['ibc'].append(balance_item)
            return result
        else:
            return None

    def serialize_staked_balance(self, liquid_response):
        if liquid_response and len(liquid_response.get('delegation_responses', [])):
            delegations = liquid_response.get('delegation_responses')
            result = []
            already_added_denoms = []
            for delegation in delegations:
                if delegation.get('balance').get('amount') != '0':
                    current_denom = delegation.get('balance').get('denom')
                    if current_denom not in already_added_denoms:
                        already_added_denoms.append(current_denom)
                        result.append({
                            'denom': current_denom,
                            'amount': float(delegation.get('balance').get('amount'))
                        })
                    else:
                        result[already_added_denoms.index(current_denom)]['amount'] += float(delegation.get('balance').get('amount'))
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(result)
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            return result_with_logos
        else:
            return None

    def serialize_unbonding_balance(self, liquid_response):
        total_unbonding = 0
        if liquid_response and len(liquid_response.get('unbonding_responses', [])):
            for unbonding_item in liquid_response.get('unbonding_responses'):
                for entry in unbonding_item.get('entries'):
                    current_time = datetime.datetime.now().timestamp()
                    if dateutil.parser.parse(entry.get('completion_time')).timestamp() > current_time:
                        total_unbonding += int(entry.get('balance'))
            if total_unbonding > 0:
                result = [{
                    'amount': total_unbonding,
                    'denom': STAKED_DENOM
                }]
                prettified_result = self.balance_prettifier_service.prettify_balance_structure(result)
                result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
                result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
                return result_with_logos
            else:
                return None
        else:
            return None

    def serialize_rewards_balance(self, liquid_response):
        if liquid_response and len(liquid_response.get('total', [])):
            result = liquid_response.get('total')
            for balance_item in result:
                balance_item['amount'] = float(balance_item['amount'])
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(result)
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            return result_with_logos
        else:
            return None

    def balance_items_mappers(self, item_type):
        mapper = {
            'liquid': self.serialize_liquid_balance,
            'staked': self.serialize_staked_balance,
            'unbonding': self.serialize_unbonding_balance,
            'rewards': self.serialize_rewards_balance,
        }
        return mapper.get(item_type)

    def get_account_balance_2(self, address: str) -> dict:
        balances_responses = asyncio.run(self.bronbro_api_client.get_account_balances(address))
        result = {}
        for balance_response in balances_responses:
            type = balance_response.get('type')
            serializer = self.balance_items_mappers(type)
            result[type] = serializer(balance_response['response'])
        return result

    def get_annual_provision(self) -> int:
        response = self.bronbro_api_client.get_annual_provisions()
        return int(float(response.get('annual_provisions'))) if response else 0

    def get_community_tax(self) -> float:
        return self.db_client.get_actual_distribution_param('community_tax').value

    def get_bonded_tokens_amount(self) -> int:
        staking_pool = self.db_client.get_staking_pool()
        return staking_pool.bonded_tokens if staking_pool else 0

    def get_account_info_balance_item(self, amount, token_info: dict) -> dict:
        result = copy.deepcopy(token_info)
        result['amount'] = amount
        return result

    def get_account_info(self, address) -> dict:
        account_staked_balance = self.get_account_staked_balance(address)
        validators = self.get_validators(address)
        if account_staked_balance:
            delegations_sum = next((balance.get('amount') for balance in account_staked_balance if balance.get('denom') == STAKED_DENOM), 0)
        else:
            delegations_sum = 0
        annual_provision = self.get_annual_provision()
        community_tax = self.get_community_tax()
        bonded_tokens_amount = self.get_bonded_tokens_amount()
        apr = annual_provision * (1 - community_tax) / bonded_tokens_amount
        total_annual_provision = sum([x['coin']['amount'] * apr * (1 - x['commission']) for x in validators])
        staked_denom_info = self.balance_prettifier_service.get_and_build_token_info(STAKED_DENOM)
        return {
            "apr": apr,
            "voting_power": delegations_sum / bonded_tokens_amount,
            "rpde": self.get_account_info_balance_item(total_annual_provision / 365.3, staked_denom_info),
            "staked": self.get_account_info_balance_item(delegations_sum, staked_denom_info),
            "annual_provision": self.get_account_info_balance_item(total_annual_provision, staked_denom_info)
        }

    def add_mintscan_avatar_to_validators(self, validators):
        for validator in validators:
            validator['mintscan_avatar'] = f'{MINTSCAN_AVATAR_URL}/cosmostation/chainlist/main/chain/cosmos/moniker/{validator.get("operator_address")}.png'
        return validators

    def get_validators(self, address):
        validators = self.db_client.get_validators(address)
        validators = [validator._asdict() for validator in validators]
        for validator in validators:
            validator['coin'] = json.loads(validator['coin'])
        validators = self.add_mintscan_avatar_to_validators(validators)
        return validators

    def get_votes(self, address, proposal_id):
        votes = self.db_client.get_account_votes(address, proposal_id)
        return [vote._asdict() for vote in votes]
