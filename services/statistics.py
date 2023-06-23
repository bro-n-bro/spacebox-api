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
