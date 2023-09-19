from datetime import timedelta, datetime
from typing import Optional, List

import clickhouse_connect

from common.db_connector import DBConnector
from common.decorators import get_first_if_exists
from config.config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD, STAKED_DENOM
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
            SELECT sum(JSONExtractInt(coin, 'amount')) as amount, JSONExtractString(coin, 'denom') as denom FROM spacebox.delegation FINAL
            WHERE delegator_address = '{address}' and JSONExtractInt(coin, 'amount') > 0
            GROUP BY denom
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
            website,
            security_contact,
            details,
            commission,
            max_change_rate,
            max_rate
        FROM (
            SELECT * FROM spacebox.delegation FINAL
            WHERE delegator_address = '{address}' AND JSONExtractInt(coin, 'amount') > 0
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
                      sum(tupleElement(JSONExtract(coins, 'Array(Tuple(denom String, amount Int64))'), 'amount')[1]) AS deposit
              FROM spacebox.proposal_deposit_message FINAL
              GROUP BY proposal_id) AS deposit ON _t.id = deposit.proposal_id
            LEFT JOIN
              ( SELECT *
              FROM spacebox.proposal_tally_result FINAL) AS tally ON _t.id = tally.proposal_id
            LEFT JOIN (
                SELECT proposal_id, init_deposit FROM (
                    SELECT proposal_id, tupleElement(JSONExtract(coins, 'Array(Tuple(denom String, amount Int64))'), 'amount')[1] AS init_deposit, rank() OVER(PARTITION BY proposal_id ORDER BY height ASC) RowNumber
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
                      sum(tupleElement(JSONExtract(coins, 'Array(Tuple(denom String, amount Int64))'), 'amount')[1]) AS deposit
              FROM spacebox.proposal_deposit_message FINA
              GROUP BY proposal_id) AS deposit ON spacebox.proposal.id = deposit.proposal_id
            LEFT JOIN
              ( SELECT *
              FROM spacebox.proposal_tally_result FINAL) AS tally ON spacebox.proposal.id = tally.proposal_id
            LEFT JOIN (
                SELECT proposal_id, init_deposit FROM (
                    SELECT proposal_id, tupleElement(JSONExtract(coins, 'Array(Tuple(denom String, amount Int64))'), 'amount')[1] AS init_deposit, rank() OVER(PARTITION BY proposal_id ORDER BY height ASC) RowNumber
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
                           JSONExtractInt(coin, 'amount')                AS coin_amount,
                           Rank()
                             over(
                               PARTITION BY operator_address, delegator_address
                               ORDER BY height DESC ) AS delegator_rank
                    FROM   spacebox.delegation FINAL
                    WHERE  JSONExtractInt(coin, 'amount') > 0) AS d
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
                        sum(amount) desc
                      ) AS voting_power_rank,
                        if(
                        voting_power_rank <= (
                        SELECT
                            active_set
                        FROM
                            (
                            SELECT
                                DISTINCT height,
                                JSONExtractInt(params, 'historical_entries') AS active_set
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
                            JSONExtractInt(coin, 'amount') AS amount
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
        ) AND JSONExtractInt(coin, 'amount') > 0 order by height DESC
        """)

    @get_first_if_exists
    def get_validator_self_delegation(self, operator_address, self_delegate_address) -> namedtuple:
        return self.make_query(f"""
            SELECT * from spacebox.delegation WHERE JSONExtractInt(coin, 'amount') > 0 and delegator_address = '{self_delegate_address}' and operator_address = '{operator_address}' order by height DESC
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
                        sum(amount) AS voting_power,
                        row_number() over(
                    ORDER BY
                        sum(amount) desc) AS voting_power_rank,
                        if(voting_power_rank <= (
                        SELECT
                            active_set
                        FROM
                            (
                            SELECT
                                JSONExtractInt(params, 'historical_entries') AS active_set
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
                            JSONExtractInt(coin, 'amount') AS amount
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
                v.consensus_address = vs.consensus_address
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
    def get_actual_distribution_param(self, parameter):
        return self.make_query(f"""
                SELECT JSONExtractInt(params, '{parameter}') as value FROM spacebox.distribution_params FINAL ORDER BY height DESC limit 1
            """)
    @get_first_if_exists
    def get_count_of_active_proposals(self) -> namedtuple:
        return self.make_query("""
            SELECT COUNT(*) FROM spacebox.proposal FINAL WHERE status = 'PROPOSAL_STATUS_VOTING_PERIOD'
        """)

    @get_first_if_exists
    def get_count_of_pending_proposals(self) -> namedtuple:
        return self.make_query("""
            SELECT COUNT(*) FROM spacebox.proposal FINAL WHERE status IN ('PROPOSAL_STATUS_VOTING_PERIOD', 'PROPOSAL_STATUS_DEPOSIT_PERIOD')
        """)

    @get_first_if_exists
    def get_last_block_height(self) -> namedtuple:
        return self.make_query("""
            SELECT MAX(height) FROM spacebox.block FINAL
        """)

    def get_blocks_lifetime(self) -> List[namedtuple]:
        return self.make_query("""
        select 
          t1.height as x, 
          coalesce(
            timestampdiff(
              SECOND, t1.timestamp, t2.timestamp
            ), 
            0
          ) as y 
        from 
          spacebox.block t1 FINAL 
          left join spacebox.block t2 on t1.height = t2.height - 1 
        order by 
          t1.height DESC 
        LIMIT 1000 OFFSET 1
        """)

    def get_transactions_per_block(self, limit, offset):
        if not limit:
            limit = 10
        if not offset:
            offset = 0
        return self.make_query(f"""
            SELECT height, timestamp, num_txs, total_gas FROM spacebox.block b FINAL ORDER BY height DESC LIMIT {limit} OFFSET {offset}
        """)

    @get_first_if_exists
    def get_actual_staking_param(self, parameter):
        return self.make_query(f"""
            SELECT JSONExtractInt(params, '{parameter}') as value FROM spacebox.staking_params FINAL ORDER BY height DESC limit 1
        """)

    @get_first_if_exists
    def get_total_supply_by_day(self, day):
        next_day = day + timedelta(days=1)
        return self.make_query(f"""
            SELECT (AVG(sp.not_bonded_tokens) + AVG(sp.bonded_tokens)) AS total_supply FROM  spacebox.staking_pool AS sp FINAL
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON sp.height  = b.height
            WHERE b.timestamp >= '{str(day)}' AND b.timestamp < '{str(next_day)}'
        """)

    def get_default_group_order_where_for_statistics(self):
        return f"""
            GROUP by x
            ORDER BY x
        """

    def get_total_supply_by_days(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, (AVG(toFloat64(sp.not_bonded_tokens)) + AVG(toFloat64(sp.bonded_tokens))) AS y 
            from (
               select * FROM spacebox.staking_pool FINAL WHERE height between {height_from} and {height_to}
            ) AS sp
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON sp.height = b.height
            {self.get_join_with_dates(from_date, to_date, grouping_function)}
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_blocks(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            select {grouping_function}(timestamp) as x, avg(lifetime) as y from (
	            select
                  t1.timestamp as timestamp,
                  coalesce(
                    timestampdiff(
                      SECOND, t1.timestamp, t2.timestamp
                    ), 
                    0
                  ) as lifetime
                from 
                  spacebox.block t1 FINAL 
                  left join spacebox.block t2 on t1.height = t2.height - 1 
                  where t1.height between {height_from} and {height_to} and lifetime > 0
                order by 
                  t1.height DESC 
                offset 1
            )
            group by x
            order by x
        """)

    def get_bonded_tokens_by_days(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, AVG(sp.bonded_tokens) AS y 
            from (
            select * FROM spacebox.staking_pool FINAL WHERE height between {height_from} and {height_to}
            ) AS sp
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON sp.height = b.height
            {self.get_join_with_dates(from_date, to_date, grouping_function)}
            {self.get_default_group_order_where_for_statistics()}
        """)

    @get_first_if_exists
    def get_actual_staking_pool(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.staking_pool FINAL ORDER BY height DESC LIMIT 1
        """)

    @get_first_if_exists
    def get_total_supply_actual(self):
        return self.make_query(f"""
            select JSONExtractInt(coin, 'amount') as amount from (
            SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractString(coins))) as coin from (
            select * from spacebox.supply order by height desc limit 1) where JSONExtractString(coin, 'denom') = '{STAKED_DENOM}')
        """)

    def get_unbonded_tokens_by_days(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, AVG(sp.not_bonded_tokens) AS y 
            from (
            select * FROM spacebox.staking_pool FINAL WHERE height between {height_from} and {height_to}
            ) AS sp
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON sp.height = b.height
            {self.get_join_with_dates(from_date, to_date, grouping_function)}
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_circulating_supply_by_days(self, from_date, to_date, detailing, height_from=None, height_to=None):
        return self.make_query(f"""
            select {detailing}(u.hh) as x, avg(JSONExtractFloat(coin, 'amount')) as y from
            (
                select arrayJoin(JSONExtractArrayRaw(JSONExtractString(coins))) as coin, height 
                from spacebox.supply AS s FINAL 
                where JSONExtractString(coin, 'denom') = '{STAKED_DENOM}' and  height between {height_from} and {height_to}
            ) as s
            LEFT JOIN (SELECT * FROM spacebox.block FINAL) AS b ON s.height = b.height
            {self.get_join_with_dates(from_date, to_date, detailing)}
            group by x
            order by x
        """)

    @get_first_if_exists
    def get_supply_actual(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.supply ORDER BY height DESC LIMIT 1
        """)

    @get_first_if_exists
    def get_community_pool_actual(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.community_pool ORDER BY height DESC LIMIT 1
        """)

    def get_bonded_ratio_by_days(self, from_date, to_date, detailing, height_from=None, height_to=None):
        return self.make_query(f"""
            SELECT {detailing}(u.hh) AS x, AVG(bonded_ratio)*100 AS y 
            from (
            select * FROM spacebox.annual_provision  FINAL WHERE height between {height_from} and {height_to}
            ) AS ap
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON ap.height = b.height
            {self.get_join_with_dates(from_date, to_date, detailing)}         
            {self.get_default_group_order_where_for_statistics()}
        """)


    def get_community_pool_by_days(self, from_date, to_date, detailing, height_from=None, height_to=None):
        return self.make_query(f"""
            SELECT {detailing}(u.hh) AS x, AVG(toFloat64(replaceAll(replaceAll(JSON_QUERY(JSONExtractString(coins, -1), '$.amount'), '[', ''), ']', ''))) AS y 
            from (
            select * FROM spacebox.community_pool FINAL WHERE height between {height_from} and {height_to}
            ) AS cp
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON cp.height = b.height
            {self.get_join_with_dates(from_date, to_date, detailing)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_inflation_by_days(self, from_date, to_date, detailing, height_from=None, height_to=None):
        return self.make_query(f"""
            SELECT {detailing}(u.hh) AS x, AVG(inflation) AS y 
            from (
            select * FROM spacebox.annual_provision FINAL WHERE height between {height_from} and {height_to}
            ) AS ap
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON ap.height = b.height
            {self.get_join_with_dates(from_date, to_date, detailing)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_annual_provision_by_days(self, from_date, to_date, detailing, height_from=None, height_to=None):
        return self.make_query(f"""
            SELECT {detailing}(u.hh) AS x, AVG(annual_provisions) AS y 
            from (
            select * FROM spacebox.annual_provision FINAL WHERE height between {height_from} and {height_to}
            ) AS ap
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON ap.height = b.height
            {self.get_join_with_dates(from_date, to_date, detailing)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    @get_first_if_exists
    def get_actual_annual_provision(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.annual_provision FINAL ORDER BY height DESC LIMIT 1
        """)

    @get_first_if_exists
    def get_actual_distribution_params(self):
        return self.make_query(f"""
            SELECT params FROM spacebox.distribution_params FINAL ORDER BY height DESC LIMIT 1
        """)

    @get_first_if_exists
    def get_actual_mint_params(self):
        return self.make_query(f"""
            SELECT params FROM spacebox.mint_params FINAL LIMIT 1
        """)

    @get_first_if_exists
    def get_one_block(self, offset):
        return self.make_query(f"""
            SELECT * from spacebox.block FINAL ORDER BY height DESC LIMIT 1 OFFSET {offset}
        """)

    @get_first_if_exists
    def get_total_accounts_actual(self):
        return self.make_query(f"""
            SELECT COUNT(DISTINCT address) as total_value from spacebox.account FINAL
        """)

    @get_first_if_exists
    def get_total_accounts_before_height(self, height):
        return self.make_query(f"""
            SELECT COUNT(DISTINCT address) as total_value from spacebox.account FINAL where height < {height}
        """)

    def get_popular_transactions_for_last_30_days(self):
        return self.make_query(f"""
            SELECT m.type as type, COUNT(*) as amount FROM spacebox.transaction AS t FINAL
                LEFT JOIN spacebox.message AS m 
                    ON t.hash = m.transaction_hash 
                LEFT JOIN spacebox.block b 
                    ON t.height = b.height 
            WHERE DATE(b.timestamp) >= DATE(NOW()) - INTERVAL 30 DAY
            GROUP BY m.type
            ORDER BY amount DESC
        """)

    def get_staked_statistics(self):
        return self.make_query(f"""
            SELECT 
              COUNT(*) as total_value, 
              CASE 
                  WHEN staked_amount < 100 THEN 0
                  WHEN staked_amount BETWEEN 100 AND 500 THEN 100 
                  WHEN staked_amount BETWEEN 501 AND 2000 THEN 500 
                  WHEN staked_amount BETWEEN 2001 AND 5000 THEN 2000 
                  WHEN staked_amount BETWEEN 5001 AND 10000 THEN 5000 
                  WHEN staked_amount BETWEEN 10001 AND 20000 THEN 10000 
                  WHEN staked_amount BETWEEN 20001 AND 50000 THEN 20000 
                  WHEN staked_amount BETWEEN 50001 AND 100000 THEN 50000 
                  WHEN staked_amount BETWEEN 100001 AND 500000 THEN 100000 
                  WHEN staked_amount > 500000 THEN 500000
              END 
              AS gap 
            FROM 
              (
                SELECT 
                  SUM(
                    JSONExtractInt(coin, 'amount')
                  ) AS staked_amount, 
                  delegator_address 
                FROM 
                  spacebox.delegation FINAL
                GROUP BY 
                  delegator_address
              ) 
            WHERE 
              staked_amount > 0 
            GROUP BY 
              gap
            ORDER BY gap
        """)

    @get_first_if_exists
    def get_amount_of_inactive_accounts(self):
        return self.make_query(f"""
            SELECT count(*) AS total_amount FROM spacebox.account FINAL WHERE address NOT IN (
                SELECT t.signer FROM spacebox.transaction AS t FINAL
                LEFT JOIN spacebox.block AS b ON b.height = t.height
                WHERE DATE(b.timestamp) >= DATE(NOW()) - INTERVAL 365 DAY
            )
        """)

    def get_new_accounts(self, from_date, to_date, grouping_function, height_from=None, height_to=None):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, count(*) as y 
            from (
            select * FROM spacebox.account FINAL WHERE height between {height_from} and {height_to}
            ) AS a
            LEFT JOIN spacebox.block b ON b.height = a.height 
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_gas_paid(self, from_date, to_date, grouping_function, height_from=None, height_to=None):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, SUM(total_gas) as y 
            from (
            select * FROM spacebox.block FINAL where height between {height_from} and {height_to}
            ) as b
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    @get_first_if_exists
    def get_gas_paid_actual(self):
        return self.make_query(f"""
            select sum(total_gas) as value from spacebox.block FINAL
        """)

    def get_transactions(self, from_date, to_date, grouping_function, height_from=None, height_to=None):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, count(*) as y 
            from (
            select * FROM spacebox.transaction FINAL where height between {height_from} and {height_to}
            ) AS t
            LEFT JOIN spacebox.block b ON b.height = t.height 
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    @get_first_if_exists
    def get_transactions_actual(self):
        return self.make_query(f"""
            select count(*) as value from spacebox.transaction FINAL
        """)

    def get_redelegation_message(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, SUM(JSONExtractInt(coin, 'amount')) as y 
            from (
            select * FROM spacebox.redelegation_message FINAL where height between {height_from} and {height_to}
            ) AS dm
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON dm.height = b.height
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_unbonding_message(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, SUM(JSONExtractInt(coin, 'amount')) as y 
            from (
            select * FROM spacebox.unbonding_delegation_message FINAL where height between {height_from} and {height_to}
            ) AS dm
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON dm.height = b.height
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_delegation_message(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(u.hh) AS x, SUM(JSONExtractInt(coin, 'amount')) AS y 
            from (
            select * FROM spacebox.delegation_message FINAL where height between {height_from} and {height_to}
            ) AS dm
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON dm.height = b.height
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_active_accounts(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            SELECT x , count(*) as y FROM (
                SELECT DISTINCT ON ({grouping_function}(u.hh) AS x, signer AS y) x, y 
                from (
                    select * FROM spacebox.transaction FINAL where height between {height_from} and {height_to}
                    ) AS t
                    LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON t.height = b.height
                    {self.get_join_with_dates(from_date, to_date, grouping_function)}       
                )
            GROUP BY x
            ORDER BY x
        """)

    @get_first_if_exists
    def get_active_accounts_actual(self, height_from):
        return self.make_query(f"""
            select count(DISTINCT signer) as value from spacebox.transaction FINAL WHERE height > {height_from}
        """)

    @get_first_if_exists
    def get_new_accounts_actual(self, height_from):
        return self.make_query(f"""
            select count(*) as value from spacebox.account final where height > {height_from}
        """)

    @get_first_if_exists
    def get_block_by_height(self, height):
        return self.make_query(f"""
            select * from spacebox.block FINAL where height={height}
        """)

    @get_first_if_exists
    def get_all_staking_parameters(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.staking_params  ORDER BY height DESC LIMIT 1
        """)

    @get_first_if_exists
    def get_all_mint_parameters(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.mint_params  ORDER BY height DESC LIMIT 1
        """)

    @get_first_if_exists
    def get_all_gov_parameters(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.gov_params  ORDER BY height DESC LIMIT 1
        """)

    @get_first_if_exists
    def get_all_distribution_parameters(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.distribution_params  ORDER BY height DESC LIMIT 1
        """)

    def get_validators_list(self, limit, offset):
        if not limit:
            limit = 10
        if not offset:
            offset = 0
        return self.make_query(f"""
            select 
                v.operator_address AS operator_address, 
                v.consensus_address AS consensus_address, 
                vd.moniker AS moniker,  
                vi.self_delegate_address AS self_delegate_address,
                vc.commission AS commission,
                vc.max_change_rate AS max_change_rate,
                vc.max_rate AS max_rate,
                vr.rank as rank,
                vr.voting_power as voting_power,
                CONCAT(v.operator_address, vi.self_delegate_address) AS concat_operator_self_delegate_addresses
            FROM spacebox.validator_status AS vs FINAL
            LEFT JOIN (SELECT * FROM spacebox.validator  FINAL) AS v ON v.consensus_address = vs.consensus_address
            LEFT JOIN (SELECT * FROM spacebox.validator_info  FINAL) AS vi ON vi.operator_address = v.operator_address
            LEFT JOIN (SELECT * FROM spacebox.validator_description  FINAL) AS vd ON vd.operator_address = v.operator_address
            LEFT JOIN (SELECT * FROM spacebox.validator_commission  FINAL) AS vc ON vc.operator_address = v.operator_address
            LEFT JOIN (
                select 
                    validator_address, 
                    voting_power, 
                    ROW_NUMBER() OVER(ORDER BY voting_power DESC) AS rank 
                from spacebox.validator_voting_power FINAL 
                where height = (select height from spacebox.validator_voting_power order by height DESC limit 1)
            ) as vr ON vr.validator_address = v.consensus_address
            WHERE vs.status = 3 and vs.jailed = FALSE
            ORDER BY vd.moniker
        """)

    @get_first_if_exists
    def get_validator_by_operator_address(self, operator_address):
        return self.make_query(f"""
            SELECT 
                v.operator_address AS operator_address, 
                v.consensus_address AS consensus_address, 
                vd.moniker AS moniker,  
                vd.website AS website,  
                vi.self_delegate_address AS self_delegate_address,
                vc.commission AS commission,
                vc.max_change_rate AS max_change_rate,
                vc.max_rate AS max_rate,
                CONCAT(v.operator_address, vi.self_delegate_address) AS concat_operator_self_delegate_addresses,
                b.timestamp as creation_time
            FROM spacebox.validator AS v FINAL 
            LEFT JOIN (SELECT * FROM spacebox.validator_info  FINAL) AS vi ON vi.operator_address = v.operator_address
            LEFT JOIN (SELECT * FROM spacebox.validator_description  FINAL) AS vd ON vd.operator_address = v.operator_address
            LEFT JOIN (SELECT * FROM spacebox.validator_commission  FINAL) AS vc ON vc.operator_address = v.operator_address
            LEFT JOIN (SELECT * FROM spacebox.create_validator_message  FINAL) AS cvm ON cvm.validator_address = v.operator_address
            LEFT JOIN (SELECT * FROM spacebox.block FINAL) AS b ON cvm.height = b.height
            WHERE operator_address = '{operator_address}'
        """)

    def get_validators_voting_power(self, validators_addresses):
        return self.make_query(f"""
            SELECT operator_address, sum(JSONExtractInt(coin, 'amount')) AS amount FROM spacebox.delegation d FINAL 
            WHERE operator_address IN ('{"','".join(validators_addresses)}')
            GROUP BY operator_address
        """)

    def get_validators_commission_earned(self, validators_addresses):
        # TODO: DISCUSS FINAL FOR IT , +10 SEC
        return self.make_query(f"""
            select operator as operator_address, sum(JSONExtractFloat(amount, 'amount')) as amount from spacebox.distribution_commission FINAL
            where operator IN ('{"','".join(validators_addresses)}')
            group by operator_address
        """)

    @get_first_if_exists
    def get_validator_voting_power(self, operator_address):
        return self.get_validators_voting_power([operator_address])

    @get_first_if_exists
    def get_validator_voting_power_and_rank(self, operator_address):
        return self.make_query(f"""
            select voting_power, rank from (
            select 
                validator_address, 
                voting_power, 
                ROW_NUMBER() OVER(ORDER BY voting_power DESC) AS rank 
            from spacebox.validator_voting_power FINAL 
            where height = (select height from spacebox.validator_voting_power order by height DESC limit 1))
            where validator_address = '{operator_address}'
        """)

    @get_first_if_exists
    def get_validator_possible_proposals(self, validator_created_at):
        return self.make_query(f"""
            SELECT COUNT(*) as value FROM spacebox.proposal FINAL where submit_time > '{validator_created_at}'
        """)


    def get_validators_self_delegations(self, concat_operator_self_delegate_addresses):
        return self.make_query(f"""
            SELECT 
                CONCAT(operator_address, delegator_address) as concat_operator_self_delegate_addresses, 
                sum(JSONExtractInt(coin, 'amount')) AS amount 
            FROM spacebox.delegation d FINAL 
            WHERE concat_operator_self_delegate_addresses IN ('{"','".join(concat_operator_self_delegate_addresses)}')
            GROUP BY concat_operator_self_delegate_addresses
        """)

    @get_first_if_exists
    def get_validator_self_delegations(self, concat_operator_self_delegate_address):
        return self.get_validators_self_delegations([concat_operator_self_delegate_address])

    def get_validators_votes(self, validators_self_delegator_addresses):
        return self.make_query(f"""
            SELECT voter,  count (*) AS value FROM
                (
                    SELECT DISTINCT ON (voter, proposal_id) * FROM spacebox.proposal_vote_message FINAL 
                    WHERE voter IN ('{"','".join(validators_self_delegator_addresses)}')
                )
            GROUP BY voter
        """)

    def get_validators_uptime_stats(self):
        return self.make_query(f"""
            select validator_address, count(*) / 10000 as value from (
                select DISTINCT ON (height, validator_address) * 
                from spacebox.validator_precommit vp FINAL 
                where height > (select max(height) from spacebox.validator_precommit) - 10000
            ) 
            group by validator_address
        """)

    @get_first_if_exists
    def get_uptime_stats_by_consensus_address(self, validator_address):
        return self.make_query(f"""
                    select validator_address, count(*) / 10000 as value from (
                        select DISTINCT ON (height, validator_address) * 
                        from spacebox.validator_precommit vp FINAL 
                        where height > (select max(height) from spacebox.validator_precommit) - 10000
                        and validator_address = '{validator_address}'
                    ) 
                    group by validator_address
                """)

    @get_first_if_exists
    def get_validator_votes(self, validators_self_delegator_address):
        return self.get_validators_votes([validators_self_delegator_address])

    def get_validators_slashing(self, consensus_addresses):
        return self.make_query(f"""
            SELECT 
                operator_address as address, 
                count(*) AS count, 
                sum(JSONExtractInt(burned, 'amount')) AS amount 
            FROM spacebox.handle_validator_signature hvs FINAL
            WHERE address in ('{"','".join(consensus_addresses)}')
            GROUP BY address 
        """)

    @get_first_if_exists
    def get_validator_slashing(self, consensus_address):
        return self.get_validators_slashing([consensus_address])

    def get_validators_delegators_count(self, validators):
        return self.make_query(f"""
            SELECT operator_address, count(*) as value FROM 
                (
                    SELECT DISTINCT ON (operator_address, delegator_address) * FROM spacebox.delegation FINAL
                    WHERE operator_address IN ('{"','".join(validators)}')
                )
            GROUP BY operator_address
        """)

    @get_first_if_exists
    def get_validator_delegators_count(self, operator_address):
        return self.get_validators_delegators_count([operator_address])

    @get_first_if_exists
    def get_block_30_days_ago(self):
        return self.make_query(f"""
            SELECT * FROM spacebox.block FINAL WHERE DATE(timestamp) >= DATE(NOW()) - INTERVAL 30 DAY ORDER BY height LIMIT 1
        """)

    def get_validators_new_delegators(self, validators, height):
        return self.make_query(f"""
            SELECT operator_address, count(*) as value FROM 
            (
                SELECT DISTINCT ON (operator_address, delegator_address) * from spacebox.delegation
                WHERE operator_address IN ('{"','".join(validators)}')
                ORDER BY height desc
            )
            WHERE height >= {height}
            GROUP BY operator_address
        """)

    @get_first_if_exists
    def get_validator_new_delegators(self, operator_address, height):
        return self.get_validators_new_delegators([operator_address], height)

    def generate_dates(self, from_date, to_date, grouping_function):
        mapper = {
            'toStartOfHour': 3600,
            'DATE': 3600*24,
            'toStartOfWeek': 3600*24*7,
            'toStartOfMonth': 3600*24*8
        }
        step = mapper[grouping_function]
        to_date_plus_day = str((datetime.strptime(to_date, "%Y-%m-%d").date() + timedelta(days=1)))
        if grouping_function == 'toStartOfMonth':
            return f"""
            select DISTINCT (toStartOfMonth(hh)) as hh from (
            WITH 
                toStartOfDay(toDate('{from_date}')) AS start,
                toStartOfDay(toDate('{to_date_plus_day}')) AS end
            SELECT arrayJoin(arrayMap(x -> toDateTime(x), range(toUInt32(start), toUInt32(end), {step}))) as hh
            )
            """
        elif grouping_function == 'toStartOfWeek':
            return f"""
            select toStartOfWeek(hh) as hh from (
            WITH 
                toStartOfDay(toDate('{from_date}')) AS start,
                toStartOfDay(toDate('{to_date_plus_day}')) AS end
            SELECT arrayJoin(arrayMap(x -> toDateTime(x), range(toUInt32(start), toUInt32(end), {step}))) as hh
            )
            """
        else:
            return f"""
            WITH 
                toStartOfDay(toDate('{from_date}')) AS start,
                toStartOfDay(toDate('{to_date_plus_day}')) AS end
            SELECT arrayJoin(arrayMap(x -> toDateTime(x), range(toUInt32(start), toUInt32(end), {step}))) as hh
            where hh < now()
            """

    def get_join_with_dates(self, from_date, to_date, grouping_function):
        return f"""
            RIGHT JOIN ({self.generate_dates(from_date, to_date, grouping_function)}) as u 
            on {grouping_function}(u.hh) = {grouping_function}(b.timestamp)
        """

    def get_validator_commissions(self, from_date, to_date, grouping_function, operator_address, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(timestamp) AS x, sum(JSONExtractFloat(amount, 'amount')) AS y 
            from (
            select * FROM spacebox.distribution_commission FINAL where height between {height_from} and {height_to} and operator_address = '{operator_address}'
            ) AS dp
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON dp.height = b.height
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_validator_rewards(self, from_date, to_date, grouping_function, operator_address, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(timestamp) AS x, sum(JSONExtractFloat(amount, 'amount')) AS y 
            from (
            select * FROM spacebox.distribution_reward FINAL where height between {height_from} and {height_to} and operator_address = '{operator_address}'
            ) AS dp
            LEFT JOIN (
                        SELECT * FROM spacebox.block  FINAL
                    ) AS b ON dp.height = b.height
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    @get_first_if_exists
    def get_min_date_height(self, date):
        return self.make_query(f"""
            SELECT height from spacebox.block FINAL
            WHERE DATE(timestamp) = '{date}'
            ORDER BY height
        """)

    @get_first_if_exists
    def get_max_date_height(self, date):
        return self.make_query(f"""
            SELECT height from spacebox.block FINAL
            WHERE DATE(timestamp) = '{date}'
            ORDER BY height DESC
        """)

    @get_first_if_exists
    def get_latest_height(self):
        return self.make_query(f"""
            SELECT height from spacebox.block FINAL
            ORDER BY height
        """)

    def get_validator_historical_uptime_stat(self, from_date, to_date, grouping_function, validator_address, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(timestamp) AS x, count(*) AS y 
            from (
            select DISTINCT ON (height, validator_address) * from spacebox.validator_precommit FINAL 
            where validator_address = '{validator_address}' 
            and height between {height_from} and {height_to}
            ) AS b
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    def get_total_count_of_blocks_for_historical_uptime_stat(self, from_date, to_date, grouping_function, height_from, height_to):
        return self.make_query(f"""
            SELECT {grouping_function}(timestamp) AS x, count(*) AS y 
            from (
            select DISTINCT ON (height) * from spacebox.validator_precommit FINAL 
            where height between {height_from} and {height_to}
            ) AS b
            {self.get_join_with_dates(from_date, to_date, grouping_function)}         
            {self.get_default_group_order_where_for_statistics()}
        """)

    @get_first_if_exists
    def get_validator(self, operator_address):
        return self.make_query(f"""
            select * from spacebox.validator FINAL where operator_address  = '{operator_address}'
        """)
