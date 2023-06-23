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

    def get_account_votes(self, account, proposal_id):
        additional_filter = ''
        if proposal_id:
            additional_filter = f'AND proposal_id = {proposal_id}'
        return self.make_query(f"""
            SELECT
                proposal_id, 
                tx_hash , 
                `option` , 
                spacebox.block.timestamp
            FROM
                spacebox.proposal_vote_message
            LEFT JOIN spacebox.block ON
                spacebox.proposal_vote_message.height = spacebox.block.height
            WHERE voter = '{account}'
            {additional_filter}
            ORDER BY spacebox.block.timestamp desc
        """)

    def get_validators_delegators_votes_info_for_proposal(self, proposal_id, validator_address=None):
        validator_filter = ''
        if validator_address:
            validator_filter = f"and operator_address = '{validator_address}'"
        return self.make_query(f"""
            SELECT operator_address,
                   proposal_id,
                   option,
                   Count() as amount_value,
                   SUM(coin_amount) as shares_value
            FROM   (SELECT *,
                           coin.amount                AS coin_amount,
                           Rank()
                             over(
                               PARTITION BY operator_address, delegator_address
                               ORDER BY height DESC ) AS delegator_rank
                    FROM   spacebox.delegation FINAL
                    WHERE  coin.amount > 0) AS d
                   left join (SELECT *
                              FROM   (SELECT *,
                                             Rank ()
                                               over (
                                                 PARTITION BY voter, proposal_id
                                                 ORDER BY height DESC ) AS vote_rank
                                      FROM   spacebox.proposal_vote_message FINAL
                                      WHERE  proposal_id = {proposal_id})) AS v
                          ON d.delegator_address = v.voter
            WHERE  delegator_rank = 1
                   AND vote_rank = 1
                   {validator_filter}
            GROUP  BY operator_address,
                      proposal_id,
                      option
            ORDER  BY operator_address 
        """)

    def get_validators_proposal_votes_with_additional_info(self, proposal_id, validator_option=None, validator_address=None):
        option_filter = ''
        validator_filter = ''
        if validator_option:
            option_filter = f"AND option = '{validator_option}' "
        if validator_address:
            validator_filter = f"AND operator_address = '{validator_address}'"
        return self.make_query(f"""
            SELECT
                _t.operator_address AS operator_address,
                _t.voting_power_rank AS voting_power_rank,
                _t.moniker AS moniker,
                pvm.option as validator_option,
                pvm.tx_hash as vote_tx_hash,
                t.self_delegate_address as self_delegate_address
            FROM
                (
                SELECT
                    *
                FROM
                    (
                    SELECT
                        operator_address,
                        row_number() over(
                    ORDER BY
                        sum("amount.1") desc
                      ) AS voting_power_rank,
                        if(
                        voting_power_rank <= (
                        SELECT
                            "active_set.4"
                        FROM
                            (
                            SELECT
                                DISTINCT height,
                                untuple(params) AS active_set
                            FROM
                                spacebox.staking_params FINAL
                            ORDER BY
                                height DESC
                            LIMIT 
                                1
                            )
                        ),
                        TRUE,
                        FALSE
                      ) AS is_active
                    FROM
                        (
                        SELECT
                            DISTINCT height,
                            operator_address,
                            untuple(coin) AS amount
                        FROM
                            spacebox.delegation FINAL
                        ORDER BY
                            height DESC
                      ) AS _d
                    GROUP BY
                        operator_address
                  ) AS _t
                LEFT JOIN (
                    SELECT
                        DISTINCT height,
                        *
                    FROM
                        spacebox.validator_description FINAL
                    ORDER BY
                        height DESC
                  ) AS t ON
                    _t.operator_address = t.operator_address
                ORDER BY
                    height DESC
              ) AS _t
            LEFT JOIN (
                SELECT
                    DISTINCT height,
                    *
                FROM
                    spacebox.validator_info FINAL
                ORDER BY
                    height DESC) AS t ON
                _t.operator_address = t.operator_address
            LEFT JOIN (
                select
                    *
                from
                    (
                    select
                        *,
                        RANK () OVER (
                        PARTITION BY voter,
                        proposal_id
                    ORDER BY 
                            height DESC
                    ) as rank
                    FROM
                        spacebox.proposal_vote_message FINAL
                    where
                        proposal_id = {proposal_id} {option_filter}
                )
            ) AS pvm ON
                t.self_delegate_address = pvm.voter
            where
                pvm.rank in (0,1)
                {validator_filter}
                ORDER BY _t.voting_power_rank DESC
        """)

    def get_validators_delegations(self) -> namedtuple:
        return self.make_query(f"""
            SELECT DISTINCT on (delegator_address, operator_address) * from spacebox.delegation WHERE delegator_address  in (
            SELECT self_delegate_address from spacebox.validator_info vi 
        ) AND coin.amount > 0 order by height DESC
        """)

    @get_first_if_exists
    def get_validator_self_delegation(self, operator_address, self_delegate_address) -> namedtuple:
        return self.make_query(f"""
            SELECT * from spacebox.delegation WHERE coin.amount > 0 and delegator_address = '{self_delegate_address}' and operator_address = '{operator_address}' order by height DESC
        """)

    @get_first_if_exists
    def get_validator_info(self, validator_address) -> namedtuple:
        return self.make_query(f"""
            SELECT
                _t.operator_address AS operator_address,
                _t.voting_power AS voting_power,
                _t.voting_power_rank AS voting_power_rank,
                _t.is_active AS is_active,
                _t.moniker AS moniker,
                _t.identity AS identity,
                _t.avatar_url AS avatar_url,
                _t.website AS website,
                _t.security_contact AS security_contact,
                _t.details AS details,
                t.self_delegate_address AS self_delegate_address,
                t.min_self_delegation AS min_self_delegation,
                tt.coin AS self_bonded,
                vc.commission as commission,
                vc.max_change_rate as max_change_rate,
                vc.max_rate as max_rate,
                vs.status as status,
                vs.jailed as jailed
            FROM
                (
                SELECT
                    *
                FROM
                    (
                    SELECT
                        operator_address,
                        sum("amount.1") AS voting_power,
                        row_number() over(
                    ORDER BY
                        sum("amount.1") desc) AS voting_power_rank,
                        if(voting_power_rank <= (
                        SELECT
                            "active_set.4"
                        FROM
                            (
                            SELECT
                                untuple(params) AS active_set
                            FROM
                                spacebox.staking_params FINAL
                            ORDER BY
                                height DESC
                            LIMIT 1)),
                        TRUE ,
                        FALSE) AS is_active
                    FROM
                        (
                        SELECT
                            operator_address,
                            untuple(coin) AS amount
                        FROM
                            spacebox.delegation FINAL
                        WHERE operator_address = '{validator_address}'
                        ORDER BY
                            height DESC
                    ) AS _d
                    GROUP BY
                        operator_address
                ) AS _t
                LEFT JOIN (
                    SELECT
                        *
                    FROM
                        spacebox.validator_description FINAL
                    ORDER BY
                        height DESC) AS t ON
                    _t.operator_address = t.operator_address
                ORDER BY
                    height DESC
            ) AS _t
            LEFT JOIN (
                SELECT
                    *
                FROM
                    spacebox.validator_info FINAL
                ORDER BY
                    height DESC) AS t ON
                _t.operator_address = t.operator_address
            LEFT JOIN (
                SELECT
                    *
                FROM
                    spacebox.delegation FINAL
                ORDER BY
                    height DESC) AS tt ON
                _t.operator_address = tt.operator_address
                AND self_delegate_address = tt.delegator_address
            LEFT JOIN (
                SELECT
                    *
                FROM
                    spacebox.validator_commission FINAL
                ORDER BY
                    height DESC) AS vc ON
                _t.operator_address = vc.operator_address
            LEFT JOIN (
                SELECT
                    *
                FROM
                    spacebox.validator FINAL
                ORDER BY
                    height DESC) AS v ON
                _t.operator_address = v.operator_address
            LEFT JOIN (
                SELECT
                    *
                FROM
                    spacebox.validator_status FINAL
                ORDER BY
                    height DESC) AS vs ON
                v.consensus_address = vs.validator_address
        """)

    @get_first_if_exists
    def get_address_votes_amount(self, voter_address) -> namedtuple:
        return self.make_query(f"""
            SELECT COUNT(DISTINCT proposal_id) 
            FROM spacebox.proposal_vote_message pvm 
            WHERE voter = '{voter_address}'
        """)

    @get_first_if_exists
    def get_staking_pool(self) -> namedtuple:
        return self.make_query(f"""
            SELECT bonded_tokens as bonded_tokens FROM spacebox.staking_pool FINAL
            WHERE height = (SELECT max(height) FROM spacebox.staking_pool FINAL)
        """)

    @get_first_if_exists
    def get_count_of_active_proposals(self) -> namedtuple:
        return self.make_query("""
            SELECT COUNT(*) FROM spacebox.proposal WHERE status = 'PROPOSAL_STATUS_VOTING_PERIOD'
        """)

    @get_first_if_exists
    def get_count_of_pending_proposals(self) -> namedtuple:
        return self.make_query("""
            SELECT COUNT(*) FROM spacebox.proposal WHERE status IN ('PROPOSAL_STATUS_VOTING_PERIOD', 'PROPOSAL_STATUS_DEPOSIT_PERIOD')
        """)

    @get_first_if_exists
    def get_last_block_height(self) -> namedtuple:
        return self.make_query("""
            SELECT MAX(height) FROM spacebox.block 
        """)
