from clients.db_client import DBClient


class StatisticsService:

    def __init__(self):
        self.db_client = DBClient()

    def get_proposals_statistics(self):
        active_proposals = self.db_client.get_count_of_active_proposals().count__
        pending_proposals = self.db_client.get_count_of_pending_proposals().count__
        return {
            'active': active_proposals,
            'pending': pending_proposals
        }

    def get_last_block_height(self):
        return {'last_block_height': self.db_client.get_last_block_height().max_height_}

    def get_blocks_time(self):
        blocks_info = self.db_client.get_blocks_lifetime()
        average_time = round(sum(item.y for item in blocks_info) / 1000, 2)
        return {
            'average_lifetime': average_time,
            'blocks': [block._asdict() for block in blocks_info]
        }

    def get_transactions_per_block(self, limit, offset):
        transactions_per_block = self.db_client.get_transactions_per_block(limit, offset)
        return [block._asdict() for block in transactions_per_block]

    def get_active_validators_and_unbound_period(self):
        result = self.db_client.get_actual_staking_params()
        return {
            'active_validators_count': result.params.get('max_validators', 0),
            'unbound_period': f"{int(result.params.get('unbonding_time', 0)/86400000000000)} days"
        }
