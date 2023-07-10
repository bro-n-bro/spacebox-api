import json
import math

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from datetime import date, timedelta

from common.constants import SECONDS_IN_YEAR


class StatisticsService:

    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()

    def get_pending_proposals_statistics(self):
        return self.db_client.get_count_of_pending_proposals().count__

    def get_active_proposals_statistics(self):
        return self.db_client.get_count_of_active_proposals().count__

    def get_last_block_height(self):
        return self.db_client.get_last_block_height().max_height_

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

    def get_active_validators(self):
        result = self.db_client.get_actual_staking_params()
        return result.params.get('max_validators', 0)


    def get_unbound_period(self):
        result = self.db_client.get_actual_staking_params()
        return f"{int(result.params.get('unbonding_time', 0)/86400000000000)} days"


    def get_token_prices(self):
        exchange_rates = self.bronbro_api_client.get_exchange_rates()
        atom_exchange_rate = next((rate for rate in exchange_rates if rate.get('symbol') == 'ATOM'), None)
        if not atom_exchange_rate:
            return {}
        prices = {
            'today': atom_exchange_rate.get('price'),
            'day_before': atom_exchange_rate.get('price') / (1 + (atom_exchange_rate.get('price_24h_change') / 100)),
            'week_before': atom_exchange_rate.get('price') / (1 + (atom_exchange_rate.get('price_7d_change') / 100)),
        }
        return prices

    def get_market_cap(self):
        prices = self.get_token_prices()
        today_total_supply = self.db_client.get_total_supply_by_day(date.today()).total_supply
        day_before_total_supply = self.db_client.get_total_supply_by_day(date.today() - timedelta(days=1)).total_supply
        week_before_total_supply = self.db_client.get_total_supply_by_day(date.today() - timedelta(days=7)).total_supply
        market_caps = {
            'today': prices.get('today') * today_total_supply if not math.isnan(today_total_supply) else None,
            'day_before': prices.get('day_before') * day_before_total_supply if not math.isnan(day_before_total_supply) else None,
            'week_before': prices.get('week_before') * week_before_total_supply if not math.isnan(week_before_total_supply) else None,
        }
        return market_caps

    def get_total_supply_by_days(self, days):
        from_date = date.today() - timedelta(days=days)
        result = self.db_client.get_total_supply_by_days(from_date)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    def get_bonded_tokens_by_days(self, days):
        from_date = date.today() - timedelta(days=days)
        result = self.db_client.get_bonded_tokens_by_days(from_date)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    def get_unbonded_tokens_by_days(self, days):
        from_date = date.today() - timedelta(days=days)
        result = self.db_client.get_unbonded_tokens_by_days(from_date)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    def get_circulating_supply_by_days(self, days):
        from_date = date.today() - timedelta(days=days)
        result = self.db_client.get_circulating_supply_by_days(from_date)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    def get_bonded_ratio_by_days(self, days):
        from_date = date.today() - timedelta(days=days)
        result = self.db_client.get_bonded_ratio_by_days(from_date)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    def get_community_pool_by_days(self, days):
        from_date = date.today() - timedelta(days=days)
        result = self.db_client.get_community_pool_by_days(from_date)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    def get_inflation_by_days(self, days):
        from_date = date.today() - timedelta(days=days)
        result = self.db_client.get_inflation_by_days(from_date)
        return [{'x': str(item.x), 'y': item.y} for item in result]

    def convert_date_diff_in_seconds(self, diff_value):
        return diff_value.seconds + diff_value.days*86400

    def get_apr_by_days(self, days):
        from_date = date.today() - timedelta(days=days)
        bonded_tokens_by_days = self.db_client.get_bonded_tokens_by_days(from_date)
        annual_provision_by_days = self.db_client.get_annual_provision_by_days(from_date)
        community_tax = json.loads(self.db_client.get_actual_distribution_params().params).get('community_tax', 0.1)
        expected_blocks_per_year = json.loads(self.db_client.get_actual_mint_params().params).get('blocks_per_year')
        block_latest = self.db_client.get_one_block(0)
        block_timestamp_latest = block_latest.timestamp
        block_height_latest = block_latest.height
        block_timestamp_20000_before = self.db_client.get_block_by_height(block_height_latest-20000).timestamp
        avg_block_lifetime = self.convert_date_diff_in_seconds(block_timestamp_latest - block_timestamp_20000_before)/20000
        real_blocks_per_year = SECONDS_IN_YEAR / avg_block_lifetime
        correction_annual_coefficient = real_blocks_per_year / expected_blocks_per_year
        result = []
        for annual_provision_in_day in annual_provision_by_days:
            bonded_tokens_in_this_day = next((bonded_tokens for bonded_tokens in bonded_tokens_by_days if bonded_tokens.x == annual_provision_in_day.x), None)
            if bonded_tokens_in_this_day:
                apr = (annual_provision_in_day.y * (1 - community_tax) / bonded_tokens_in_this_day.y) * correction_annual_coefficient
                result.append({
                    'x': str(annual_provision_in_day.x),
                    'y': apr
                })
        return result

    def get_apy_by_days(self, days):
        apr_by_days = self.get_apr_by_days(days)
        result = []
        for item in apr_by_days:
            apy = (1 + item.get('y')/365)**365 - 1
            result.append({
                'x': item.get('x'),
                'y': apy
            })
        return result

    def get_total_accounts(self):
        return self.db_client.get_total_accounts().total_value

    def get_popular_transactions(self):
        result = self.db_client.get_popular_transactions_for_last_30_days()
        return [item._asdict() for item in result]

    def get_staked_statistics(self):
        result = self.db_client.get_staked_statistics()
        return [item._asdict() for item in result]

    def get_inactive_accounts(self):
        return self.db_client.get_amount_of_inactive_accounts().total_amount

    def detailing_mapper(self, detailing):
        mapper = {
            'hour': 'toStartOfHour',
            'day': 'DATE',
            'week': 'toStartOfWeek',
            'month': 'toStartOfMonth'
        }
        return mapper.get(detailing, 'DATE')

    def get_new_accounts(self, from_date, to_date, detailing):
        group_by = self.detailing_mapper(detailing)
        result = self.db_client.get_new_accounts(from_date, to_date, group_by)
        return [item._asdict() for item in result]

    def get_gas_paid(self, from_date, to_date, detailing):
        group_by = self.detailing_mapper(detailing)
        result = self.db_client.get_gas_paid(from_date, to_date, group_by)
        return [item._asdict() for item in result]

    def get_transactions(self, from_date, to_date, detailing):
        group_by = self.detailing_mapper(detailing)
        result = self.db_client.get_transactions(from_date, to_date, group_by)
        return [item._asdict() for item in result]
