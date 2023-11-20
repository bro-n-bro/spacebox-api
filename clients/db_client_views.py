from datetime import datetime, timedelta
from typing import List
from common.db_connector import DBConnector
from collections import namedtuple

from services.sql_filter_builder import SqlFilterBuilderService


class DBClientViews:

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
            on {grouping_function}(u.hh) = {grouping_function}(a.xx)
        """

    def get_default_statistics(self, from_date, to_date, grouping_function, view, sql_merge_function):
        return self.make_query(f"""
            select {grouping_function}(u.hh) as x,
                   a.y
            from (
            SELECT {grouping_function}(timestamp_start_of_hour) AS xx,
                   {sql_merge_function}(y) AS y
            FROM spacebox.{view}
            WHERE timestamp_start_of_hour BETWEEN '{from_date}' AND '{to_date}'
            GROUP BY {grouping_function}(timestamp_start_of_hour)
            ) as a
            {self.get_join_with_dates(from_date, to_date, grouping_function)}
            ORDER BY {grouping_function}(u.hh)
        """)

    def get_default_statistics_for_validator(
            self,
            from_date,
            to_date,
            grouping_function,
            view,
            sql_merge_function,
            operator_address
    ):
        return self.make_query(f"""
            SELECT {grouping_function}(timestamp_start_of_hour) AS x,
                   {sql_merge_function}(y) AS y
            FROM spacebox.{view}
            WHERE operator_address = '{operator_address}' AND 
                  DATE(timestamp_start_of_hour) BETWEEN '{from_date}' AND '{to_date}'
            GROUP BY {grouping_function}(timestamp_start_of_hour), operator_address
            ORDER BY {grouping_function}(timestamp_start_of_hour)
        """)

    def get_fees_paid(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'test_fees_paid', 'sumMerge')

    def get_blocks_lifetime(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'avg_block_time', 'avgMerge')

    def get_total_supply(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'total_supply', 'avgMerge')

    def get_circulating_supply(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'circulating_supply', 'avgMerge')

    def get_bonded_tokens(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'bonded_tokens', 'medianMerge')

    def get_bonded_ratio(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'bonded_ratio', '100*avgMerge')

    def get_inflation(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'inflation', 'avgMerge')

    def get_community_pool(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'community_pool_view', 'avgMerge')

    def get_restake_execution_count(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'restake_execution_count', 'countMerge')

    def get_new_accounts(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'new_accounts', 'countMerge')

    def get_gas_paid(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'gas_paid', 'sumMerge')

    def get_transactions(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'transactions_view', 'countMerge')

    def get_redelegation_message(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'redelegation_message_view', 'sumMerge')

    def get_unbonding_message(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'unbonding_message_view', 'sumMerge')

    def get_delegation_message(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'delegation_message_view', 'sumMerge')

    def get_active_accounts(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'active_accounts_view', 'countMerge')

    def get_restake_token_amount(self, from_date, to_date, grouping_function):
        return self.get_default_statistics(from_date, to_date, grouping_function, 'restake_token_amount', 'sumMerge')

    def get_statistics_validator_commission(self, from_date, to_date, grouping_function, operator_address):
        return self.get_default_statistics_for_validator(
            from_date,
            to_date,
            grouping_function,
            'commission_earned',
            'sumMerge',
            operator_address
        )

    def get_statistics_validator_rewards(self, from_date, to_date, grouping_function, operator_address):
        return self.get_default_statistics_for_validator(
            from_date,
            to_date,
            grouping_function,
            'rewards_earned',
            'sumMerge',
            operator_address
        )

    def get_statistics_validator_voting_power(self, from_date, to_date, grouping_function, operator_address):
        return self.get_default_statistics_for_validator(
            from_date,
            to_date,
            grouping_function,
            'voting_power_view',
            'medianMerge',
            operator_address
        )

    def get_total_commission_earned_validator(self, validator):
        return self.make_query(f"""
            SELECT sumMerge(y) AS y
            FROM spacebox.commission_earned
            WHERE operator_address = '{validator}'
        """)

    def get_total_rewards_earned_validator(self, validator):
        return self.make_query(f"""
            SELECT sumMerge(y) AS y
            FROM spacebox.rewards_earned
            WHERE operator_address = '{validator}'
        """)
