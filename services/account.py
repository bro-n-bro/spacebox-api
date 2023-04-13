from typing import Optional

from clients.db_client import DBClient
from clients.lcd_api_client import LcdApiClient
from config.config import STAKED_DENOM


class AccountService:

    def __init__(self):
        self.db_client = DBClient()
        self.lcd_api_client = LcdApiClient()

    def get_account_liquid_balance(self, address: str) -> Optional[dict]:
        account_balance = self.db_client.get_account_balance(address)
        if account_balance:
            return {account_balance.coins.get('denom')[0]: account_balance.coins.get('amount')[0]}
        else:
            return None

    def get_account_staked_balance(self, address: str) -> dict:
        staked_balance = self.db_client.get_stacked_balance_for_address(address)
        total_sum = staked_balance.sum_coin_amount_ if staked_balance else 0
        return {STAKED_DENOM: total_sum}

    def get_account_unbonding_balance(self, address: str) -> dict:
        unbonding_balance = self.db_client.get_unbonding_balance_for_address(address)
        total_sum = unbonding_balance.sum_coin_amount_ if unbonding_balance else 0
        return {STAKED_DENOM: total_sum}

    def get_account_commission_balance(self, address: str) -> dict:
        api_response = self.lcd_api_client.get_address_rewards(address)
        if api_response:
            rewards = next((int(float(r['amount'])) for r in api_response['total'] if r['denom'] == STAKED_DENOM), 0)
        else:
            rewards = 0
        return {STAKED_DENOM: rewards}

    def get_account_balance(self, address: str) -> dict:
        account_balance = {
            'liquid': self.get_account_liquid_balance(address),
            'staked': self.get_account_staked_balance(address),
            'unbonding': self.get_account_unbonding_balance(address),
            'rewards': self.get_account_commission_balance(address),
        }
        return account_balance

    def get_annual_provision(self) -> int:
        response = self.lcd_api_client.get_annual_provisions()
        return int(float(response.get('annual_provisions'))) if response else 0

    def get_community_tax(self) -> float:
        distribution_params = self.db_client.get_distribution_params()
        return distribution_params.params.get('community_tax') if distribution_params else 0

    def get_bonded_tokens_amount(self) -> int:
        staking_pool = self.db_client.get_staking_pool()
        return staking_pool.bonded_tokens if staking_pool else 0

    def get_account_info(self, address) -> dict:
        account_staked_balance = self.get_account_staked_balance(address)
        validators = self.get_validators(address)
        delegations_sum = account_staked_balance[STAKED_DENOM]
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
