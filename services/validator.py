import json

from clients.db_client import DBClient
from common.decorators import history_statistics_handler
from config.config import MINTSCAN_AVATAR_URL


class ValidatorService:

    def __init__(self):
        self.db_client = DBClient()

    def get_validators(self, limit, offset):
        validators = self.db_client.get_validators_list(limit, offset)
        operator_addresses = [validator.operator_address for validator in validators]
        concat_operator_self_delegate_addresses = [validator.concat_operator_self_delegate_addresses for validator in validators]
        self_delegate_addresses = [validator.self_delegate_address for validator in validators]
        consensus_addresses = [validator.consensus_address for validator in validators]
        block_30_days_ago_height = self.db_client.get_block_30_days_ago().height
        # validators_voting_power = self.db_client.get_validators_voting_power(operator_addresses)
        # validators_commission_earned = self.db_client.get_validators_commission_earned(operator_addresses)
        validators_self_delegations = self.db_client.get_validators_self_delegations(concat_operator_self_delegate_addresses)
        validators_votes = self.db_client.get_validators_votes(self_delegate_addresses)
        validators_slashing = self.db_client.get_validators_slashing(consensus_addresses)
        validators_delegators = self.db_client.get_validators_delegators_count(operator_addresses)
        validators_new_delegators = self.db_client.get_validators_new_delegators(operator_addresses, block_30_days_ago_height)
        result = [validator._asdict() for validator in validators]
        for validator in result:
            validator['mintscan_avatar_url'] = f'{MINTSCAN_AVATAR_URL}/cosmostation/chainlist/main/chain/cosmos/moniker/{validator.get("operator_address")}.png'
            # for index, commission_earned in enumerate(validators_commission_earned):
            #     if commission_earned.operator_address == validator.get('operator_address'):
            #         commission_earned = validators_commission_earned.pop(index)
            #         validator['commission_earned'] = commission_earned.amount
            for index, self_delegations in enumerate(validators_self_delegations):
                if self_delegations.concat_operator_self_delegate_addresses == validator.get('concat_operator_self_delegate_addresses'):
                    self_delegations = validators_self_delegations.pop(index)
                    validator['self_delegations'] = self_delegations.amount
                    del validator['concat_operator_self_delegate_addresses']
            for index, votes in enumerate(validators_votes):
                if votes.voter == validator.get('self_delegate_address'):
                    votes = validators_votes.pop(index)
                    validator['votes'] = votes.value
            for index, slashing in enumerate(validators_slashing):
                if slashing.address == validator.get('consensus_address'):
                    slashing = validators_slashing.pop(index)
                    validator['slashing'] = slashing.count
            for index, delegators in enumerate(validators_delegators):
                if delegators.operator_address == validator.get('operator_address'):
                    delegators = validators_delegators.pop(index)
                    validator['delegators'] = delegators.value
            for index, new_delegators in enumerate(validators_new_delegators):
                if new_delegators.operator_address == validator.get('operator_address'):
                    new_delegators = validators_new_delegators.pop(index)
                    validator['new_delegators'] = new_delegators.value
        for validator in result:
            if 'slashing' not in validator:
                validator['slashing'] = 0
            if 'new_delegators' not in validator:
                validator['new_delegators'] = 0
            if 'delegators' not in validator:
                validator['delegators'] = 0
            if 'votes' not in validator:
                validator['votes'] = 0
            if 'self_delegations' not in validator:
                validator['self_delegations'] = 0
        return result

    def get_validator_by_operator_address(self, operator_address):
        validator = self.db_client.get_validator_by_operator_address(operator_address)
        if not validator:
            return {}
        result = validator._asdict()
        block_30_days_ago_height = self.db_client.get_block_30_days_ago().height
        voting_power_and_rank = self.db_client.get_validator_voting_power_and_rank(validator.consensus_address)
        self_delegations = self.db_client.get_validator_self_delegations(validator.concat_operator_self_delegate_addresses)
        votes = self.db_client.get_validator_votes(validator.self_delegate_address)
        slashing = self.db_client.get_validator_slashing(validator.consensus_address)
        delegators = self.db_client.get_validator_delegators_count(validator.operator_address)
        new_delegators = self.db_client.get_validator_new_delegators(validator.operator_address, block_30_days_ago_height)
        del result['concat_operator_self_delegate_addresses']
        result['voting_power'] = voting_power_and_rank.voting_power if voting_power_and_rank else None
        result['rank'] = voting_power_and_rank.rank if voting_power_and_rank else None
        result['self_delegations'] = self_delegations.amount if self_delegations else None
        result['votes'] = votes.value if votes else None
        result['slashing'] = slashing.count if slashing else None
        result['delegators'] = delegators.value if delegators else None
        result['new_delegators'] = new_delegators.value if new_delegators else None
        result['available_proposals'] = self.db_client.get_validator_possible_proposals(str(validator.creation_time)).value
        result['mintscan_avatar_url'] = f'{MINTSCAN_AVATAR_URL}/cosmostation/chainlist/main/chain/cosmos/moniker/{result.get("operator_address")}.png'
        return result

    @history_statistics_handler
    def get_validator_commissions(self, from_date, to_date, detailing, operator_address, height_from=None, height_to=None):
        return self.db_client.get_validator_commissions(from_date, to_date, detailing, operator_address, height_from, height_to)

    @history_statistics_handler
    def get_validator_rewards(self, from_date, to_date, detailing, operator_address, height_from=None, height_to=None):
        return self.db_client.get_validator_rewards(from_date, to_date, detailing, operator_address, height_from, height_to)

    def get_validator_info(self, validator_address):
        validator = self.db_client.get_validator_info(validator_address)
        if validator is None:
            return {}
        validator_votes = self.db_client.get_address_votes_amount(validator.self_delegate_address)
        result = validator._asdict()
        result['proposals_voted_amount'] = validator_votes.uniqExact_proposal_id_
        # TODO: check self bonded for this address cosmosvaloper1n5pu2rtz4e2skaeatcmlexza7kheedzh8a2680
        result['self_bonded'] = json.loads(validator.self_bonded) if validator.self_bonded else {}
        result['delegator_shares'] = result['self_bonded'].get('amount', 0) / validator.voting_power if validator.voting_power > 0 else None
        result['mintscan_avatar_url'] = f'{MINTSCAN_AVATAR_URL}/cosmostation/chainlist/main/chain/cosmos/moniker/{validator_address}.png'
        result['avatar_url'] = f'{MINTSCAN_AVATAR_URL}/cosmostation/chainlist/main/chain/cosmos/moniker/{validator_address}.png'
        return result
