from typing import Optional, List

import clickhouse_connect

from common.db_connector import DBConnector
from common.decorators import get_first_if_exists
from config.config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD
from collections import namedtuple

from services.sql_filter_builder import SqlFilterBuilderService


class DBClient:

    def __init__(self):
        self.connection = DBConnector().clickhouse_client
        self.sql_filter_builder = SqlFilterBuilderService()

    def fix_column_names(self, column_names: List[str]) -> List[str]:
        res = []
        for column_name in column_names:
            new_column_name = column_name.replace('(', '_').replace(')', '_').replace('.', '_')
            res.append(new_column_name)
        return res

    def make_query(self, query: str) -> List[namedtuple]:
        query = self.connection.query(query)
        Record = namedtuple("Record", self.fix_column_names(query.column_names))
        result = [Record(*item) for item in query.result_rows]
        return result

    @get_first_if_exists
    def get_account_balance(self, address: str) -> Optional[namedtuple]:
        return self.make_query(f'''
            SELECT address, coins FROM spacebox.account_balance FINAL
            WHERE address = '{address}'
            LIMIT 1
        ''')

    def get_stacked_balance_for_address(self, address: str) -> namedtuple:
        return self.make_query(f'''
            SELECT sum(coin.amount), coin.denom FROM spacebox.delegation FINAL
            WHERE delegator_address = '{address}' and coin.amount > 0
            GROUP BY coin.denom
        ''')

    def get_unbonding_balance_for_address(self, address: str) -> namedtuple:
        return self.make_query(f'''
            SELECT sum(coin.amount), coin.denom FROM spacebox.unbonding_delegation FINAL
            WHERE delegator_address = '{address}'
            AND completion_timestamp > now()
            GROUP BY coin.denom
        ''')

    @get_first_if_exists
    def get_distribution_params(self) -> Optional[namedtuple]:
        return self.make_query(f'''
        select * from spacebox.distribution_params final
        order by height desc
        limit 1
    ''')

    @get_first_if_exists
    def get_staking_pool(self) -> Optional[namedtuple]:
        return self.make_query(f'''
        select * from spacebox.staking_pool final
        order by height desc
        limit 1
    ''')

    def get_validators(self, address: str) -> List[namedtuple]:
        return self.make_query(f'''
        SELECT 
            t.operator_address as operator_address,
            moniker,
            delegator_address,
            coin,
            identity,
            avatar_url,
            website,
            security_contact,
            details,
            commission,
            max_change_rate,
            max_rate
        FROM (
            SELECT * FROM spacebox.delegation FINAL
            WHERE delegator_address = '{address}' AND coin.amount > 0
        ) AS _t
        LEFT JOIN (
            SELECT * FROM spacebox.validator_description FINAL
        ) AS t ON _t.operator_address = t.operator_address
        LEFT JOIN (
            SELECT * FROM spacebox.validator_commission FINAL
        ) AS c ON _t.operator_address = c.operator_address 
    ''')

    def get_proposals(self, limit, offset, query_params) -> List[namedtuple]:
        if not limit:
            limit = 10
        if not offset:
            offset = 0
        filter_string = self.sql_filter_builder.build_filter(query_params)
        return self.make_query(f'''
            SELECT 
                id,
                title,
                description,
                content,
                proposal_route,
                proposal_type,
                submit_time,
                deposit_end_time,
                voting_start_time,
                voting_end_time,
                proposer_address,
                status,
                deposit,
                tally.yes,
                tally.abstain,
                tally.no,
                tally.no_with_veto,
                init_deposit,
                VOTE_OPTION_YES,
                VOTE_OPTION_NO,
                VOTE_OPTION_ABSTAIN,
                VOTE_OPTION_NO_WITH_VETO
            FROM (
                SELECT * FROM spacebox.proposal FINAL
                {filter_string}
                ) as _t
            LEFT JOIN
              ( SELECT proposal_id,
                      sum(tupleElement(coins,1)[1]) AS deposit
              FROM spacebox.proposal_deposit_message FINAL
              GROUP BY proposal_id) AS deposit ON _t.id = deposit.proposal_id
            LEFT JOIN
              ( SELECT *
              FROM spacebox.proposal_tally_result FINAL) AS tally ON _t.id = tally.proposal_id
            LEFT JOIN (
                SELECT proposal_id, init_deposit FROM (
                    SELECT proposal_id, tupleElement(coins,1)[1] AS init_deposit, rank() OVER(PARTITION BY proposal_id ORDER BY height ASC) RowNumber
                    FROM spacebox.proposal_deposit_message FINAL
                ) AS init_deposit
                WHERE RowNumber = 1
            ) AS init_deposit ON _t.id = init_deposit.proposal_id
            LEFT JOIN (
                SELECT 
                    yes.proposal_id as proposal_id,
                    VOTE_OPTION_YES,
                    VOTE_OPTION_NO,
                    VOTE_OPTION_ABSTAIN,
                    VOTE_OPTION_NO_WITH_VETO
                FROM (
                    SELECT proposal_id, count(*) AS VOTE_OPTION_YES FROM spacebox.proposal_vote_message FINAL
                    WHERE option = 'VOTE_OPTION_YES'
                    GROUP BY proposal_id, option
                ) AS yes
                LEFT JOIN (
                    SELECT proposal_id, count(*) AS VOTE_OPTION_NO FROM spacebox.proposal_vote_message FINAL
                    WHERE option = 'VOTE_OPTION_NO'
                    GROUP BY proposal_id, option
                ) AS no ON yes.proposal_id = no.proposal_id
                LEFT JOIN (
                    SELECT proposal_id, count(*) AS VOTE_OPTION_ABSTAIN FROM spacebox.proposal_vote_message FINAL
                    WHERE option = 'VOTE_OPTION_ABSTAIN'
                    GROUP BY proposal_id, option
                ) AS abstain ON yes.proposal_id = abstain.proposal_id
                LEFT JOIN (
                    SELECT proposal_id, count(*) AS VOTE_OPTION_NO_WITH_VETO FROM spacebox.proposal_vote_message FINAL
                    WHERE option = 'VOTE_OPTION_NO_WITH_VETO'
                    GROUP BY proposal_id, option
                ) AS nwv ON yes.proposal_id = nwv.proposal_id
            ) AS votes_count ON _t.id = votes_count.proposal_id
            WHERE deposit > 999999
            ORDER BY id DESC
                        LIMIT {limit} OFFSET {offset}
        ''')

    @get_first_if_exists
    def get_proposal(self, id: int) -> Optional[namedtuple]:
        return self.make_query(f'''
            SELECT 
                id,
                title,
                description,
                content,
                proposal_route,
                proposal_type,
                submit_time,
                deposit_end_time,
                voting_start_time,
                voting_end_time,
                proposer_address,
                status,
                deposit,
                tally.yes,
                tally.abstain,
                tally.no,
                tally.no_with_veto,
                init_deposit,
                VOTE_OPTION_YES,
                VOTE_OPTION_NO,
                VOTE_OPTION_ABSTAIN,
                VOTE_OPTION_NO_WITH_VETO
            FROM spacebox.proposal FINAL
            LEFT JOIN
              ( SELECT proposal_id,
                      sum(tupleElement(coins,1)[1]) AS deposit
              FROM spacebox.proposal_deposit_message FINA
              GROUP BY proposal_id) AS deposit ON spacebox.proposal.id = deposit.proposal_id
            LEFT JOIN
              ( SELECT *
              FROM spacebox.proposal_tally_result FINAL) AS tally ON spacebox.proposal.id = tally.proposal_id
            LEFT JOIN (
                SELECT proposal_id, init_deposit FROM (
                    SELECT proposal_id, tupleElement(coins,1)[1] AS init_deposit, rank() OVER(PARTITION BY proposal_id ORDER BY height ASC) RowNumber
                    FROM spacebox.proposal_deposit_message FINAL
                ) AS init_deposit
                WHERE RowNumber = 1
            ) AS init_deposit ON spacebox.proposal.id = init_deposit.proposal_id
            LEFT JOIN (
                SELECT 
                    yes.proposal_id as proposal_id,
                    VOTE_OPTION_YES,
                    VOTE_OPTION_NO,
                    VOTE_OPTION_ABSTAIN,
                    VOTE_OPTION_NO_WITH_VETO
                FROM (
                    SELECT proposal_id, count(*) AS VOTE_OPTION_YES FROM spacebox.proposal_vote_message FINAL
                    WHERE option = 'VOTE_OPTION_YES'
                    GROUP BY proposal_id, option
                ) AS yes
                LEFT JOIN (
                    SELECT proposal_id, count(*) AS VOTE_OPTION_NO FROM spacebox.proposal_vote_message FINAL
                    WHERE option = 'VOTE_OPTION_NO'
                    GROUP BY proposal_id, option
                ) AS no ON yes.proposal_id = no.proposal_id
                LEFT JOIN (
                    SELECT proposal_id, count(*) AS VOTE_OPTION_ABSTAIN FROM spacebox.proposal_vote_message FINAL
                    WHERE option = 'VOTE_OPTION_ABSTAIN'
                    GROUP BY proposal_id, option
                ) AS abstain ON yes.proposal_id = abstain.proposal_id
                LEFT JOIN (
                    SELECT proposal_id, count(*) AS VOTE_OPTION_NO_WITH_VETO FROM spacebox.proposal_vote_message FINAL
                    WHERE option = 'VOTE_OPTION_NO_WITH_VETO'
                    GROUP BY proposal_id, option
                ) AS nwv ON yes.proposal_id = nwv.proposal_id
            ) AS votes_count ON spacebox.proposal.id = votes_count.proposal_id
            WHERE id = {id}
        ''')

    def get_proposals_deposits(self, ids: List[str]) -> List[namedtuple]:

        return self.make_query(f"""
            SELECT
                proposal_id,
                depositor_address,
                tx_hash,
                coins,
                spacebox.block.timestamp
            FROM
                spacebox.proposal_deposit_message
            LEFT JOIN spacebox.block ON
                spacebox.proposal_deposit_message.height = spacebox.block.height
            WHERE
                proposal_id IN ({",".join(ids)})
        """)

    def get_proposals_ids_with_votes(self, limit, offset) -> List[namedtuple]:
        if not limit:
            limit = 10
        if not offset:
            offset = 0
        return self.make_query(f'''
            SELECT 
                DISTINCT 
                    proposal_id 
            FROM 
                spacebox.proposal_vote_message 
            ORDER BY proposal_id
            LIMIT {limit} 
            OFFSET {offset}
        ''')

    def get_amount_votes(self, proposals_ids) -> List[namedtuple]:
        return self.make_query(f'''
            SELECT 
                option, proposal_id, count(*) 
            FROM (
                SELECT 
                    * 
                FROM (
                    SELECT *, RANK() OVER(
                        PARTITION BY voter, proposal_id ORDER BY height DESC 
                    ) as rank 
                    FROM spacebox.proposal_vote_message
                    WHERE proposal_id IN ({','.join(proposals_ids)})
                ) AS _t
                WHERE rank = 1
            ) AS t
            GROUP BY option, proposal_id
        ''')

    def get_shares_votes(self, proposals_ids) -> List[namedtuple]:
        return self.make_query(f'''
            SELECT * 
            FROM spacebox.proposal_tally_result 
            WHERE proposal_id IN ({','.join(proposals_ids)})
            ORDER BY height desc
        ''')

    def get_amount_votes_for_proposal(self, proposal_id) -> List[namedtuple]:
        return self.make_query(f'''
            SELECT option, count(*) FROM (
                SELECT * FROM (
                    SELECT *, RANK() OVER(
                        PARTITION BY voter ORDER BY height DESC 
                    ) as rank FROM spacebox.proposal_vote_message
                        WHERE proposal_id = {proposal_id}
                    ) AS _t
               WHERE rank = 1
            ) AS t
            GROUP BY option
        ''')

    @get_first_if_exists
    def get_shares_votes_for_proposal(self, proposal_id) -> namedtuple:
        return self.make_query(f'''
            SELECT * 
            FROM spacebox.proposal_tally_result 
            WHERE proposal_id = {proposal_id}
            ORDER BY height desc
            LIMIT 1
        ''')
