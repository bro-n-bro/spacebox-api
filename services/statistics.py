import math

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from datetime import date, timedelta


class StatisticsService:

    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()

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

    def get_market_cap(self):
        exchange_rates = self.bronbro_api_client.get_exchange_rates()
        atom_exchange_rate = next((rate for rate in exchange_rates if rate.get('symbol') == 'ATOM'), None)
        if not atom_exchange_rate:
            return {}
        prices = {
            'today': atom_exchange_rate.get('price'),
            'day_before': atom_exchange_rate.get('price')/(1+(atom_exchange_rate.get('price_24h_change')/100)),
            'week_before': atom_exchange_rate.get('price')/(1+(atom_exchange_rate.get('price_7d_change')/100)),
        }
        today_total_supply = self.db_client.get_total_supply_by_day(date.today()).total_supply
        day_before_total_supply = self.db_client.get_total_supply_by_day(date.today() - timedelta(days=1)).total_supply
        week_before_total_supply = self.db_client.get_total_supply_by_day(date.today() - timedelta(days=7)).total_supply
        market_caps = {
            'today': prices.get('today') * today_total_supply if not math.isnan(today_total_supply) else None,
            'day_before': prices.get('day_before') * day_before_total_supply if not math.isnan(day_before_total_supply) else None,
            'week_before': prices.get('week_before') * week_before_total_supply if not math.isnan(week_before_total_supply) else None,
        }
        return {
            'prices': prices,
            'marker_caps': market_caps
        }
