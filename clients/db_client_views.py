from datetime import timedelta, datetime
from typing import Optional, List

import clickhouse_connect

from common.constants import BRONBRO_OPERATOR_ADDRESS
from common.db_connector import DBConnector
from common.decorators import get_first_if_exists
from config.config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD, STAKED_DENOM
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