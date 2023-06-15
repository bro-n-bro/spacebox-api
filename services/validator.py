from clients.db_client import DBClient
from config.config import MINTSCAN_AVATAR_URL


class ValidatorService:

    def __init__(self):
        self.db_client = DBClient()

    def get_validator_info(self, validator_address):
        validator = self.db_client.get_validator_info(validator_address)
        if validator is None:
            return {}
        validator_votes = self.db_client.get_address_votes_amount(validator.self_delegate_address)
        result = validator._asdict()
        result['proposals_voted_amount'] = validator_votes.uniqExact_proposal_id_
        result['delegator_shares'] = validator.self_bonded.get('amount', 0) / validator.voting_power if validator.voting_power > 0 else None
        result['mintscan_avatar_url'] = f'{MINTSCAN_AVATAR_URL}/cosmostation/chainlist/main/chain/cosmos/moniker/{validator_address}.png'
        return result
