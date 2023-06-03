from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from config.config import STAKED_DENOM
from services.balance_prettifier import BalancePrettifierService


class DistributionService:

    def __init__(self):
        self.db_client = DBClient()
        self.balance_prettifier_service = BalancePrettifierService()
        self.bronbro_api_client = BronbroApiClient()

    def get_staking_pool(self):
        staking_pool = self.db_client.get_staking_pool()
        result = self.balance_prettifier_service.get_and_build_token_info(STAKED_DENOM)
        result['amount'] = staking_pool.bonded_tokens if staking_pool else 0
        return result
