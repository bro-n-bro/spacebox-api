import json

from clients.db_client import DBClient
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
        validators_voting_power = self.db_client.get_validators_voting_power(operator_addresses)
        validators_self_delegations = self.db_client.get_validators_self_delegations(concat_operator_self_delegate_addresses)
        validators_votes = self.db_client.get_validators_votes(self_delegate_addresses)
        validators_slashing = self.db_client.get_validators_slashing(consensus_addresses)
        validators_delegators = self.db_client.get_validators_delegators_count(operator_addresses)
        validators_new_delegators = self.db_client.get_validators_new_delegators(operator_addresses, block_30_days_ago_height)
        result = [validator._asdict() for validator in validators]
        for validator in result:
            for index, voting_power in enumerate(validators_voting_power):
                if voting_power.operator_address == validator.get('operator_address'):
                    voting_power = validators_voting_power.pop(index)
                    validator['voting_power'] = voting_power._asdict()
            for index, self_delegations in enumerate(validators_self_delegations):
                if self_delegations.concat_operator_self_delegate_addresses == validator.get('concat_operator_self_delegate_addresses'):
                    self_delegations = validators_self_delegations.pop(index)
                    validator['self_delegations'] = self_delegations._asdict()
                    del validator['concat_operator_self_delegate_addresses']
                    del validator['self_delegations']['concat_operator_self_delegate_addresses']
            for index, votes in enumerate(validators_votes):
                if votes.voter == validator.get('self_delegate_address'):
                    votes = validators_votes.pop(index)
                    validator['votes'] = votes._asdict()
            for index, slashing in enumerate(validators_slashing):
                if slashing.address == validator.get('consensus_address'):
                    slashing = validators_slashing.pop(index)
                    validator['slashing'] = slashing._asdict()
            for index, delegators in enumerate(validators_delegators):
                if delegators.operator_address == validator.get('operator_address'):
                    delegators = validators_delegators.pop(index)
                    validator['delegators'] = delegators._asdict()
            for index, new_delegators in enumerate(validators_new_delegators):
                if new_delegators.operator_address == validator.get('operator_address'):
                    new_delegators = validators_new_delegators.pop(index)
                    validator['new_delegators'] = new_delegators._asdict()
        return result


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
        return result
