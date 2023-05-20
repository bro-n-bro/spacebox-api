from collections import namedtuple
from typing import Optional, List

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from config.config import MINTSCAN_AVATAR_URL
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

    def get_votes(self, limit: Optional[int], offset: Optional[int]):
        proposals = self.db_client.get_proposals_ids_with_votes(limit, offset)
        proposals_ids = [str(proposal.proposal_id) for proposal in proposals]
        proposals_shares_votes = self.db_client.get_shares_votes(proposals_ids)
        proposals_amount_votes = self.db_client.get_amount_votes(proposals_ids)
        result = []
        for proposal in proposals:
            shares_values = next((shares_votes for shares_votes in proposals_shares_votes if shares_votes.proposal_id == proposal.proposal_id), None)
            proposal_info = {
                'id': proposal.proposal_id,
                'amount_option_yes': next((amount_vote.count__ for amount_vote in proposals_amount_votes if amount_vote.proposal_id==proposal.proposal_id and amount_vote.option == 'VOTE_OPTION_YES'), 0),
                'amount_option_no': next((amount_vote.count__ for amount_vote in proposals_amount_votes if amount_vote.proposal_id==proposal.proposal_id and amount_vote.option == 'VOTE_OPTION_NO'), 0),
                'amount_option_abstain': next((amount_vote.count__ for amount_vote in proposals_amount_votes if amount_vote.proposal_id==proposal.proposal_id and amount_vote.option == 'VOTE_OPTION_ABSTAIN'), 0),
                'amount_option_nwv': next((amount_vote.count__ for amount_vote in proposals_amount_votes if amount_vote.proposal_id==proposal.proposal_id and amount_vote.option == 'VOTE_OPTION_NO_WITH_VETO'), 0),
                'shares_option_yes': shares_values.yes if shares_values else 0,
                'shares_option_no': shares_values.no if shares_values else 0,
                'shares_option_abstain': shares_values.abstain if shares_values else 0,
                'shares_option_nvw': shares_values.no_with_veto if shares_values else 0,
            }
            result.append(proposal_info)
        return result

    def get_vote(self, proposal_id):
        shares_votes = self.db_client.get_shares_votes_for_proposal(proposal_id)
        amount_votes = self.db_client.get_amount_votes_for_proposal(proposal_id)
        result = {
            'id': int(proposal_id),
            'amount_option_yes': next((amount_vote.count__ for amount_vote in amount_votes if amount_vote.option == 'VOTE_OPTION_YES'), 0),
            'amount_option_no': next((amount_vote.count__ for amount_vote in amount_votes if amount_vote.option == 'VOTE_OPTION_NO'), 0),
            'amount_option_abstain': next((amount_vote.count__ for amount_vote in amount_votes if amount_vote.option == 'VOTE_OPTION_ABSTAIN'), 0),
            'amount_option_nwv': next((amount_vote.count__ for amount_vote in amount_votes if amount_vote.option == 'VOTE_OPTION_NO_WITH_VETO'), 0),
            'shares_option_yes': shares_votes.yes if shares_votes else 0,
            'shares_option_no': shares_votes.no if shares_votes else 0,
            'shares_option_abstain': shares_votes.abstain if shares_votes else 0,
            'shares_option_nvw': shares_votes.no_with_veto if shares_votes else 0,
        }
        return result

    def build_validator_info_for_proposal(self, validator_info, delegators_info):
        result = validator_info._asdict()
        no_vote = next((delegator for delegator in delegators_info if delegator.option == 'VOTE_OPTION_NO'), None)
        no_with_veto_vote = next((delegator for delegator in delegators_info if delegator.option == 'VOTE_OPTION_NO_WITH_VETO'), None)
        abstain_vote = next((delegator for delegator in delegators_info if delegator.option == 'VOTE_OPTION_ABSTAIN'), None)
        yes_vote = next((delegator for delegator in delegators_info if delegator.option == 'VOTE_OPTION_YES'), None)

        no_vote_amount = no_vote.amount_value if no_vote else 0
        no_with_veto_vote_amount = no_with_veto_vote.amount_value if no_with_veto_vote else 0
        abstain_vote_amount = abstain_vote.amount_value if abstain_vote else 0
        yes_vote_amount = yes_vote.amount_value if yes_vote else 0
        total_vote_amount = no_vote_amount + no_with_veto_vote_amount + abstain_vote_amount + yes_vote_amount
        if no_with_veto_vote_amount > total_vote_amount / 3:
            most_voted = 'VOTE_OPTION_NO_WITH_VETO'
        else:
            max_votes = max(no_vote_amount, no_with_veto_vote_amount, abstain_vote_amount, yes_vote_amount)
            if max_votes == no_vote_amount:
                most_voted = 'VOTE_OPTION_NO'
            elif max_votes == no_with_veto_vote_amount:
                most_voted = 'VOTE_OPTION_NO_WITH_VETO'
            elif max_votes == yes_vote_amount:
                most_voted = 'VOTE_OPTION_YES'
            else:
                most_voted = 'VOTE_OPTION_ABSTAIN'
        result['most_voted'] = most_voted
        result['delegators_shares_option_yes'] = yes_vote.shares_value if yes_vote else 0
        result['delegators_shares_option_no'] = no_vote.shares_value if no_vote else 0
        result['delegators_shares_option_abstain'] = abstain_vote.shares_value if abstain_vote else 0
        result['delegators_shares_option_nwv'] = no_with_veto_vote.shares_value if no_with_veto_vote else 0
        result['delegators_amount_option_yes'] = yes_vote_amount
        result['delegators_amount_option_no'] = no_vote_amount
        result['delegators_amount_option_abstain'] = abstain_vote_amount
        result['delegators_amount_option_nwv'] = no_with_veto_vote_amount
        result['mintscan_avatar_url'] = f'{MINTSCAN_AVATAR_URL}/cosmostation/chainlist/main/chain/cosmos/moniker/{validator_info.operator_address}.png'
        return result

    def get_delegators_votes_info_for_proposal(self, proposal_id):
        result = []
        delegators_votes_info = self.db_client.get_validators_delegators_votes_info_for_proposal(proposal_id)
        validators_specific_info = self.db_client.get_validators_proposal_votes_with_additional_info(proposal_id)
        for validator in validators_specific_info:
            validator_delegators = [delegator for delegator in delegators_votes_info if delegator.operator_address == validator.operator_address]
            validator_response = self.build_validator_info_for_proposal(validator, validator_delegators)
            result.append(validator_response)
        return result
