from typing import Optional, List

from clients.db_client import DBClient
from clients.lcd_api_client import LcdApiClient
from config.config import STAKED_DENOM


class ProposalService:

    def __init__(self):
        self.db_client = DBClient()

    def get_proposals(self, limit: Optional[int], offset: Optional[int], query_params) -> List[dict]:
        proposals = self.db_client.get_proposals(limit, offset, query_params)
        return [proposal._asdict() for proposal in proposals]

    def get_proposal(self, id: int) -> dict:
        proposal = self.db_client.get_proposal(id)
        if proposal:
            return proposal._asdict()
        else:
            return {}
