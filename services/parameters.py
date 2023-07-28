import json

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from common.constants import NANOSECONDS_IN_DAY, SECONDS_IN_MINUTE
from common.in_memory_cache import get_denom_to_search_in_api


class ParametersService:

    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()

    def get_staking_params(self):
        result = json.loads(self.db_client.get_all_staking_parameters().params)
        result['unbonding_time_seconds'] = result.pop('unbonding_time') // 1000000000
        result['bond_denom'] = get_denom_to_search_in_api(result['bond_denom'])
        return result

    def get_mint_params(self):
        result = json.loads(self.db_client.get_all_mint_parameters().params)
        result['mint_denom'] = get_denom_to_search_in_api(result['mint_denom'])
        return result

    def get_distribution_params(self):
        return json.loads(self.db_client.get_all_distribution_parameters().params)

    def get_slash_params(self):
        response = self.bronbro_api_client.get_slash_params()
        result = {}
        if response:
            result = response.get('params')
            result['downtime_jail_duration_seconds'] = int(result.pop('downtime_jail_duration')[:-1])
            result['min_signed_per_window'] = float(result['min_signed_per_window'])
            result['signed_blocks_window'] = int(result['signed_blocks_window'])
            result['slash_fraction_double_sign'] = float(result['slash_fraction_double_sign'])
            result['slash_fraction_downtime'] = float(result['slash_fraction_downtime'])
        return result
