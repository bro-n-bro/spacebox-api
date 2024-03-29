import json
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
        self.VOTE_OPTION_NO = 'VOTE_OPTION_NO'
        self.VOTE_OPTION_YES = 'VOTE_OPTION_YES'
        self.VOTE_OPTION_ABSTAIN = 'VOTE_OPTION_ABSTAIN'
        self.VOTE_OPTION_NO_WITH_VETO = 'VOTE_OPTION_NO_WITH_VETO'
        self.DID_NOT_VOTE = 'DID_NOT_VOTE'
        self.WEIGHTED_VOTE = 'WEIGHTED_VOTE'

    def get_proposals(self, limit: Optional[int], offset: Optional[int], query_params) -> List[dict]:
        result = []
        proposals = self.db_client.get_proposals(limit, offset, query_params)
        if not proposals:
            return []
        proposals_ids = [str(proposal.id) for proposal in proposals]
        proposals_deposits = self.db_client.get_proposals_deposits(proposals_ids)
        for proposal in proposals:
            proposal_deposits = [item for item in proposals_deposits if item.proposal_id == proposal.id]
            prettified_deposits = self.format_proposal_deposits(proposal_deposits)
            proposal = proposal._asdict()
            proposal['depositors'] = prettified_deposits
            result.append(proposal)
        return result

    def format_proposal_deposits(self, deposits: List[namedtuple]):
        result = []
        for deposit in deposits:
            deposit = deposit._asdict()
            deposit.pop('proposal_id')
            prettified_result = self.balance_prettifier_service.prettify_balance_structure(json.loads(deposit.get('coins')))
            result_with_prices = self.balance_prettifier_service.add_additional_fields_to_balance(prettified_result)
            result_with_logos = self.balance_prettifier_service.add_logo_to_balance_items(result_with_prices)
            deposit['coins'] = result_with_logos
            result.append(deposit)
        return result

    def get_proposal(self, id: int) -> dict:
        proposal = self.db_client.get_proposal(id)
        if proposal:
            proposal_deposits = self.db_client.get_proposals_deposits([str(id)])
            prettified_deposits = self.format_proposal_deposits(proposal_deposits)
            result = proposal._asdict()
            result['content'] = json.loads(result['content'])
            result['depositors'] = prettified_deposits
            return result
        else:
            return {}

    def get_votes(self, limit: Optional[int], offset: Optional[int], order_by: Optional[str]):
        total = self.db_client.get_count_of_proposals_with_votes().result
        proposals = self.db_client.get_proposals_ids_with_votes(limit, offset, order_by)
        proposals_ids = [str(proposal.proposal_id) for proposal in proposals]
        proposals_shares_votes = self.db_client.get_shares_votes(proposals_ids)
        proposals_amount_votes = self.db_client.get_amount_votes(proposals_ids)
        proposals_info = self.db_client.get_proposals_end_time_and_status(proposals_ids)
        result = []
        for proposal in proposals:
            proposal_end_time_and_status = next((item for item in proposals_info if item.id==proposal.proposal_id), None)
            shares_values = next((shares_votes for shares_votes in proposals_shares_votes if shares_votes.proposal_id == proposal.proposal_id), None)
            proposal_info = {
                'id': proposal.proposal_id,
                'amount_option_yes': next((amount_vote.count__ for amount_vote in proposals_amount_votes if amount_vote.proposal_id==proposal.proposal_id and amount_vote.option == self.VOTE_OPTION_YES), 0),
                'amount_option_no': next((amount_vote.count__ for amount_vote in proposals_amount_votes if amount_vote.proposal_id==proposal.proposal_id and amount_vote.option == self.VOTE_OPTION_NO), 0),
                'amount_option_abstain': next((amount_vote.count__ for amount_vote in proposals_amount_votes if amount_vote.proposal_id==proposal.proposal_id and amount_vote.option == self.VOTE_OPTION_ABSTAIN), 0),
                'amount_option_nwv': next((amount_vote.count__ for amount_vote in proposals_amount_votes if amount_vote.proposal_id==proposal.proposal_id and amount_vote.option == self.VOTE_OPTION_NO_WITH_VETO), 0),
                'shares_option_yes': shares_values.yes if shares_values else 0,
                'shares_option_no': shares_values.no if shares_values else 0,
                'shares_option_abstain': shares_values.abstain if shares_values else 0,
                'shares_option_nvw': shares_values.no_with_veto if shares_values else 0,
                'voting_end_time': str(proposal_end_time_and_status.voting_end_time) if proposal_end_time_and_status else '',
                'status': proposal_end_time_and_status.status if proposal_end_time_and_status else ''
            }
            result.append(proposal_info)
        return result, total

    def get_vote(self, proposal_id):
        shares_votes = self.db_client.get_shares_votes_for_proposal(proposal_id)
        amount_votes = self.db_client.get_amount_votes_for_proposal(proposal_id)
        result = {
            'id': int(proposal_id),
            'amount_option_yes': next((amount_vote.count__ for amount_vote in amount_votes if amount_vote.option == self.VOTE_OPTION_YES), 0),
            'amount_option_no': next((amount_vote.count__ for amount_vote in amount_votes if amount_vote.option == self.VOTE_OPTION_NO), 0),
            'amount_option_abstain': next((amount_vote.count__ for amount_vote in amount_votes if amount_vote.option == self.VOTE_OPTION_ABSTAIN), 0),
            'amount_option_nwv': next((amount_vote.count__ for amount_vote in amount_votes if amount_vote.option == self.VOTE_OPTION_NO_WITH_VETO), 0),
            'shares_option_yes': shares_votes.yes if shares_votes else 0,
            'shares_option_no': shares_votes.no if shares_votes else 0,
            'shares_option_abstain': shares_votes.abstain if shares_votes else 0,
            'shares_option_nvw': shares_votes.no_with_veto if shares_votes else 0,
        }
        return result

    def build_empty_validator_answer(self, validator_address):
        return {
            'operator_address': validator_address,
            'voting_power_rank': None,
            'moniker': None,
            'validator_option': None,
            'vote_tx_hash': None,
        }

    def build_validator_info_for_proposal(self, validator_info, delegators_info, validator_address=None, validator_self_delegation=None):
        result = validator_info._asdict() if validator_info else self.build_empty_validator_answer(validator_address)
        no_vote = next((delegator for delegator in delegators_info if delegator.option == self.VOTE_OPTION_NO), None)
        no_with_veto_vote = next((delegator for delegator in delegators_info if delegator.option == self.VOTE_OPTION_NO_WITH_VETO), None)
        abstain_vote = next((delegator for delegator in delegators_info if delegator.option == self.VOTE_OPTION_ABSTAIN), None)
        yes_vote = next((delegator for delegator in delegators_info if delegator.option == self.VOTE_OPTION_YES), None)

        validator_option = {}
        if validator_info and validator_self_delegation:

            if not validator_info.validator_option:
                validator_option = {}
            elif validator_info.validator_option not in [self.VOTE_OPTION_YES, self.VOTE_OPTION_NO,
                                                       self.VOTE_OPTION_ABSTAIN, self.VOTE_OPTION_NO_WITH_VETO]:
                validator_options_list = json.loads(validator_info.validator_option)
                for option in validator_options_list:
                    if option['option'] == 1:
                        validator_option[self.VOTE_OPTION_YES] = option['weight']
                    elif option['option'] == 2:
                        validator_option[self.VOTE_OPTION_NO] = option['weight']
                    elif option['option'] == 3:
                        validator_option[self.VOTE_OPTION_NO_WITH_VETO] = option['weight']
                    elif option['option'] == 4:
                        validator_option[self.VOTE_OPTION_ABSTAIN] = option['weight']
            else:
                validator_option[validator_info.validator_option] = 1

            if yes_vote:
                yes_vote = yes_vote._replace(shares_value=yes_vote.shares_value - json.loads(validator_self_delegation.coin).get('amount')*validator_option.get(self.VOTE_OPTION_YES, 0))
                yes_vote = yes_vote._replace(amount_value=yes_vote.amount_value - validator_option.get(self.VOTE_OPTION_YES, 0))

            if no_vote:
                no_vote = no_vote._replace(shares_value=no_vote.shares_value - json.loads(validator_self_delegation.coin).get('amount')*validator_option.get(self.VOTE_OPTION_NO, 0))
                no_vote = no_vote._replace(amount_value=no_vote.amount_value - validator_option.get(self.VOTE_OPTION_NO, 0))

            if abstain_vote:
                abstain_vote = abstain_vote._replace(shares_value=abstain_vote.shares_value - json.loads(validator_self_delegation.coin).get('amount')*validator_option.get(self.VOTE_OPTION_ABSTAIN, 0))
                abstain_vote = abstain_vote._replace(amount_value=abstain_vote.amount_value - validator_option.get(self.VOTE_OPTION_ABSTAIN, 0))

            if no_with_veto_vote:
                no_with_veto_vote = no_with_veto_vote._replace(shares_value=no_with_veto_vote.shares_value - json.loads(validator_self_delegation.coin).get('amount')*validator_option.get(self.VOTE_OPTION_NO_WITH_VETO, 0))
                no_with_veto_vote = no_with_veto_vote._replace(amount_value=no_with_veto_vote.amount_value - validator_option.get(self.VOTE_OPTION_NO_WITH_VETO, 0))

        total_shares_votes = (yes_vote.shares_value if yes_vote else 0) + (no_vote.shares_value if no_vote else 0) + (
            no_with_veto_vote.shares_value if no_with_veto_vote else 0)
        yes_vote_shares = yes_vote.shares_value if yes_vote else 0
        no_with_veto_vote_shares = no_with_veto_vote.shares_value if no_with_veto_vote else 0

        if total_shares_votes == 0:
            most_voted = self.DID_NOT_VOTE
        elif no_with_veto_vote_shares > total_shares_votes / 3:
            most_voted = self.VOTE_OPTION_NO_WITH_VETO
        elif yes_vote_shares > total_shares_votes / 2:
            most_voted = self.VOTE_OPTION_YES
        else:
            most_voted = self.VOTE_OPTION_NO
        result['validator_option'] = validator_option
        result['most_voted'] = most_voted
        result['delegators_shares_option_yes'] = yes_vote.shares_value if yes_vote else 0
        result['delegators_shares_option_no'] = no_vote.shares_value if no_vote else 0
        result['delegators_shares_option_abstain'] = abstain_vote.shares_value if abstain_vote else 0
        result['delegators_shares_option_nwv'] = no_with_veto_vote.shares_value if no_with_veto_vote else 0
        result['delegators_amount_option_yes'] = yes_vote.amount_value if yes_vote else 0
        result['delegators_amount_option_no'] = no_vote.amount_value if no_vote else 0
        result['delegators_amount_option_abstain'] = abstain_vote.amount_value if abstain_vote else 0
        result['delegators_amount_option_nwv'] = no_with_veto_vote.amount_value if no_with_veto_vote else 0
        result['mintscan_avatar_url'] = f'{MINTSCAN_AVATAR_URL}/cosmostation/chainlist/main/chain/cosmos/moniker/{validator_info.operator_address if validator_info else validator_address}.png'
        return result

    def get_delegators_votes_info_for_proposal(self, proposal_id, validator_option):
        result = []
        delegators_votes_info = self.db_client.get_validators_delegators_votes_info_for_proposal(proposal_id)
        validators_specific_info = self.db_client.get_validators_proposal_votes_with_additional_info(proposal_id)
        if validator_option:
            if validator_option == self.WEIGHTED_VOTE:
                validators_specific_info = [validator for validator in validators_specific_info if
                                            validator.validator_option and validator.validator_option not in
                                            [self.VOTE_OPTION_NO, self.VOTE_OPTION_YES,self.VOTE_OPTION_NO_WITH_VETO,self.VOTE_OPTION_ABSTAIN]]
            else:
                validators_specific_info = [validator for validator in validators_specific_info if
                                            validator.validator_option == validator_option]
        validators_self_delegations = self.db_client.get_validators_delegations()
        for validator in validators_specific_info:
            validator_self_delegation = next((self_delegation for self_delegation in validators_self_delegations if self_delegation.delegator_address == validator.self_delegate_address and self_delegation.operator_address == validator.operator_address), None)
            validator_delegators = [delegator for delegator in delegators_votes_info if delegator.operator_address == validator.operator_address]
            if validator_delegators or validator.validator_option:
                validator_response = self.build_validator_info_for_proposal(validator, validator_delegators, validator_self_delegation=validator_self_delegation)
                result.append(validator_response)
        return result

    def get_validator_delegators_votes_info_for_proposal(self, proposal_id, validator_address):
        validators_specific_info = self.db_client.get_validators_proposal_votes_with_additional_info(proposal_id, validator_address=validator_address)
        if len(validators_specific_info):
            validators_specific_info = validators_specific_info[0]
            validators_self_delegations = self.db_client.get_validator_self_delegation(validators_specific_info.operator_address, validators_specific_info.self_delegate_address)
        else:
            validators_specific_info = None
            validators_self_delegations = None
        delegators_votes_info = self.db_client.get_validators_delegators_votes_info_for_proposal(proposal_id, validator_address)
        validator_response = self.build_validator_info_for_proposal(validators_specific_info, delegators_votes_info, validator_address, validators_self_delegations)
        return validator_response
