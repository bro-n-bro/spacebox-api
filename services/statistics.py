import math

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from datetime import date, timedelta


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
