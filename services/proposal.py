from collections import namedtuple
from typing import Optional, List

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from services.balance_prettifier import BalancePrettifierService


class ProposalService:

    def __init__(self):
        self.db_client = DBClient()
        self.balance_prettifier_service = BalancePrettifierService()
        self.bronbro_api_client = BronbroApiClient()

    def get_proposals(self, limit: Optional[int], offset: Optional[int], query_params) -> List[dict]:
        exchange_rates = self.bronbro_api_client.get_exchange_rates()
        result = []
        proposals = self.db_client.get_proposals(limit, offset, query_params)
        proposals_ids = [str(proposal.id) for proposal in proposals]
        proposals_deposits = self.db_client.get_proposals_deposits(proposals_ids)
        for proposal in proposals:
            proposal_deposits = [item for item in proposals_deposits if item.proposal_id == proposal.id]
            prettified_deposits = self.format_proposal_deposits(proposal_deposits, exchange_rates)
            proposal = proposal._asdict()
            proposal['depositors'] = prettified_deposits
            result.append(proposal)
        return result

    def format_proposal_deposits(self, deposits: List[namedtuple], exchange_rates: List[dict]):
        result = []
        for deposit in deposits:
            deposit = deposit._asdict()
            deposit.pop('proposal_id')
            deposit_structured_coins = []
            for i in range(len(deposit.get('coins').get('denom'))):
                deposit_structured_coins.append({
                    'denom': deposit.get('coins').get('denom')[i],
                    'amount': deposit.get('coins').get('amount')[i],
                })
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(deposit_structured_coins)
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result, exchange_rates)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            deposit['coins'] = result_with_logos
            result.append(deposit)
        return result

    def get_proposal(self, id: int) -> dict:
        exchange_rates = self.bronbro_api_client.get_exchange_rates()
        proposal = self.db_client.get_proposal(id)
        proposal_deposits = self.db_client.get_proposals_deposits([str(id)])
        prettified_deposits = self.format_proposal_deposits(proposal_deposits, exchange_rates)
        if proposal:
            result = proposal._asdict()
            result['depositors'] = prettified_deposits
            return result
        else:
            return {}
