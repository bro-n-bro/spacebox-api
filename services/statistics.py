import json
import math

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient
from datetime import date, timedelta

from clients.db_client_views import DBClientViews
from common.constants import SECONDS_IN_YEAR, NANOSECONDS_IN_DAY
from common.decorators import history_statistics_handler_for_view, history_statistics_handler


class StatisticsService:

    def __init__(self):
        self.db_client = DBClient()
        self.db_client_views = DBClientViews()
        self.bronbro_api_client = BronbroApiClient()

    def get_pending_proposals_statistics(self):
        return self.db_client.get_count_of_pending_proposals().count__

    def get_active_proposals_statistics(self):
        return self.db_client.get_count_of_active_proposals().count__

    def get_last_block_height(self):
        return self.db_client.get_last_block_height().max_height_

    def get_blocks_time(self):
        blocks_info = self.db_client.get_blocks_lifetime()
        block_latest = self.db_client.get_one_block(0)
        block_timestamp_latest = block_latest.timestamp
        block_height_latest = block_latest.height
        block_timestamp_20000_before = self.db_client.get_block_by_height(block_height_latest - 20000).timestamp
        avg_block_lifetime = self.convert_date_diff_in_seconds(block_timestamp_latest - block_timestamp_20000_before) / 20000
        return {
            'average_lifetime': avg_block_lifetime,
            'blocks': [block._asdict() for block in blocks_info]
        }

    def get_transactions_per_block(self, limit, offset):
        transactions_per_block = self.db_client.get_transactions_per_block(limit, offset)
        return [{'height': block.height, 'num_txs': block.num_txs, 'timestamp': str(block.timestamp), 'total_gas': block.total_gas} for block in transactions_per_block]

    def get_active_validators(self):
        result = self.db_client.get_actual_staking_param('max_validators')
        return result.value


    def get_unbound_period(self):
        return f"{int(self.db_client.get_actual_staking_param('unbonding_time').value / NANOSECONDS_IN_DAY)} days"


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

    def get_token_current_price(self):
        exchange_rates = self.bronbro_api_client.get_exchange_rates()
        return next((rate for rate in exchange_rates if rate.get('symbol') == 'ATOM'), None)

    def get_market_cap(self):
        market_cap = self.get_circulating_supply_actual()
        current_price = self.get_token_current_price()
        if not current_price:
            return 0
        else:
            return market_cap * current_price.get('price') / (10**current_price.get('exponent'))

    def detailing_mapper(self, detailing):
        mapper = {
            'hour': 'toStartOfHour',
            'day': 'DATE',
            'week': 'toStartOfWeek',
            'month': 'toStartOfMonth'
        }
        return mapper.get(detailing, 'DATE')

    @history_statistics_handler_for_view
    def get_total_supply_by_days(self, from_date, to_date, detailing):
        return self.db_client_views.get_total_supply(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_blocks(self, from_date, to_date, detailing):
        return self.db_client_views.get_blocks_lifetime(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_fees_paid(self, from_date, to_date, detailing):
        return self.db_client_views.get_fees_paid(from_date, to_date, detailing)

    def get_fees_paid_actual(self):
        today = str(date.today())
        height_from = self.db_client.get_min_date_height(today).height
        return self.db_client.get_fees_paid_actual(height_from).value

    def get_total_supply_actual(self):
        total_supply = self.db_client.get_total_supply_actual()
        return total_supply.amount if total_supply else None

    @history_statistics_handler_for_view
    def get_bonded_tokens_by_days(self, from_date, to_date, detailing):
        return self.db_client_views.get_bonded_tokens(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_unbonded_tokens_by_days(self, from_date, to_date, detailing):
        return self.db_client_views.get_unbonded_tokens(from_date, to_date, detailing)

    def get_unbonded_tokens_actual(self):
        return self.db_client.get_actual_staking_pool().not_bonded_tokens

    def get_bonded_tokens_actual(self):
        return self.db_client.get_actual_staking_pool().bonded_tokens

    @history_statistics_handler
    def get_circulating_supply_by_days(self, from_date, to_date, detailing, height_from=None, height_to=None):
        return self.db_client.get_circulating_supply_by_days(from_date, to_date, detailing, height_from, height_to)

    # @history_statistics_handler_for_view
    # def get_circulating_supply_by_days(self, from_date, to_date, detailing):
    #     return self.db_client_views.get_circulating_supply(from_date, to_date, detailing)

    def get_circulating_supply_actual(self):
        return self.get_total_supply_actual() - self.get_community_pool_actual()

    @history_statistics_handler_for_view
    def get_bonded_ratio_by_days(self, from_date, to_date, detailing):
        return self.db_client_views.get_bonded_ratio(from_date, to_date, detailing)

    def get_bonded_ratio_actual(self):
        return self.db_client.get_actual_annual_provision().bonded_ratio

    @history_statistics_handler_for_view
    def get_community_pool_by_days(self, from_date, to_date, detailing):
        return self.db_client_views.get_community_pool(from_date, to_date, detailing)

    def get_community_pool_actual(self):
        coins = json.loads(self.db_client.get_community_pool_actual().coins)
        return float(coins[-1].get('amount'))

    @history_statistics_handler_for_view
    def get_inflation_by_days(self, from_date, to_date, detailing):
        return self.db_client_views.get_inflation(from_date, to_date, detailing)

    def get_inflation_actual(self):
        return self.db_client.get_actual_annual_provision().inflation

    def convert_date_diff_in_seconds(self, diff_value):
        return diff_value.seconds + diff_value.days*86400

    def get_apr_by_days(self, from_date, to_date, detailing):
        group_by = self.detailing_mapper(detailing)
        bonded_tokens_by_days = self.db_client_views.get_bonded_tokens(from_date, to_date, group_by)
        annual_provision_by_days = self.db_client_views.get_annual_provision(from_date, to_date, group_by)
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
        for index, annual_provision_in_day in enumerate(annual_provision_by_days):
            bonded_tokens_in_this_day = bonded_tokens_by_days[index]
            if bonded_tokens_in_this_day:
                apr = (annual_provision_in_day.y * (1 - community_tax) / bonded_tokens_in_this_day.y) * correction_annual_coefficient
                result.append({
                    'x': str(annual_provision_in_day.x),
                    'y': apr
                })
        return result

    def get_apr_actual(self):
        bonded_tokens = self.db_client.get_actual_staking_pool().bonded_tokens
        annual_provision = self.db_client.get_actual_annual_provision().annual_provisions
        community_tax = json.loads(self.db_client.get_actual_distribution_params().params).get('community_tax', 0.1)
        expected_blocks_per_year = json.loads(self.db_client.get_actual_mint_params().params).get('blocks_per_year')
        block_latest = self.db_client.get_one_block(0)
        block_timestamp_latest = block_latest.timestamp
        block_height_latest = block_latest.height
        block_timestamp_20000_before = self.db_client.get_block_by_height(block_height_latest-20000).timestamp
        avg_block_lifetime = self.convert_date_diff_in_seconds(block_timestamp_latest - block_timestamp_20000_before)/20000
        real_blocks_per_year = SECONDS_IN_YEAR / avg_block_lifetime
        correction_annual_coefficient = real_blocks_per_year / expected_blocks_per_year
        return (annual_provision * (1 - community_tax) / bonded_tokens) * correction_annual_coefficient

    def get_apy_by_days(self, from_date, to_date, detailing):
        apr_by_days = self.get_apr_by_days(from_date, to_date, detailing)
        result = []
        for item in apr_by_days:
            apy = (1 + item.get('y')/365)**365 - 1
            result.append({
                'x': item.get('x'),
                'y': apy
            })
        return result

    def get_apy_actual(self):
        apr = self.get_apr_actual()
        return (1 + apr/365)**365 - 1

    def get_total_accounts_actual(self):
        return self.db_client.get_total_accounts_actual().total_value

    def get_total_accounts(self, from_date, to_date, detailing):
        new_accounts = self.get_new_accounts(from_date, to_date, detailing)
        height_before = self.db_client.get_min_date_height(from_date).height
        accounts_before_count = self.db_client = self.db_client.get_total_accounts_before_height(height_before).total_value
        result = []
        for item in new_accounts:
            amount_to_add = result[-1]['y'] if len(result) else accounts_before_count
            result.append({
                'x': item['x'],
                'y': item['y'] + amount_to_add
            })
        return result

    def get_popular_transactions(self):
        result = self.db_client.get_popular_transactions_for_last_30_days()
        return [item._asdict() for item in result]

    def get_staked_statistics(self):
        result = self.db_client.get_staked_statistics()
        return [item._asdict() for item in result]

    def get_wealth_distribution(self):
        result = self.db_client.get_wealth_distribution()
        return [item._asdict() for item in result]

    def get_inactive_accounts(self):
        return self.db_client.get_amount_of_inactive_accounts().total_amount

    @history_statistics_handler
    def get_new_accounts(self, from_date, to_date, detailing, height_from=None, height_to=None):
        return self.db_client.get_new_accounts(from_date, to_date, detailing, height_from, height_to)

    # @history_statistics_handler_for_view
    # def get_new_accounts(self, from_date, to_date, detailing):
    #     return self.db_client_views.get_new_accounts(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_gas_paid(self, from_date, to_date, detailing):
        return self.db_client_views.get_gas_paid(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_transactions(self, from_date, to_date, detailing):
        return self.db_client_views.get_transactions(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_redelegation_message(self, from_date, to_date, detailing):
        return self.db_client_views.get_redelegation_message(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_unbonding_message(self, from_date, to_date, detailing):
        return self.db_client_views.get_unbonding_message(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_delegation_message(self, from_date, to_date, detailing):
        return self.db_client_views.get_delegation_message(from_date, to_date, detailing)

    @history_statistics_handler_for_view
    def get_active_accounts(self, from_date, to_date, detailing):
        return self.db_client_views.get_active_accounts(from_date, to_date, detailing)

    @history_statistics_handler
    def get_restake_token_amount(self, from_date, to_date, detailing, height_from=None, height_to=None):
        return self.db_client.get_restake_token_amount(from_date, to_date, detailing, height_from, height_to)

    # @history_statistics_handler_for_view
    # def get_restake_token_amount(self, from_date, to_date, detailing):
    #     return self.db_client_views.get_restake_token_amount(from_date, to_date, detailing)

    def get_restake_token_amount_actual(self):
        today = str(date.today())
        height_from = self.db_client.get_min_date_height(today).height
        return self.db_client.get_restake_token_amount_actual(height_from).value

    def get_active_accounts_actual(self):
        today = str(date.today())
        height_from = self.db_client.get_min_date_height(today).height
        return self.db_client.get_active_accounts_actual(height_from).value

    def get_new_accounts_actual(self):
        today = str(date.today())
        height_from = self.db_client.get_min_date_height(today).height
        return self.db_client.get_new_accounts_actual(height_from).value

    def get_gas_paid_actual(self):
        return self.db_client.get_gas_paid_actual().value

    def get_transactions_actual(self):
        return self.db_client.get_transactions_actual().value

    def get_user_bronbro_staking(self, addresses):
        return self.db_client.get_user_bronbro_staking(addresses).amount

    @history_statistics_handler_for_view
    def get_restake_execution_count(self, from_date, to_date, detailing):
        return self.db_client_views.get_restake_execution_count(from_date, to_date, detailing)

    def get_restake_execution_count_actual(self):
        today = str(date.today())
        height_from = self.db_client.get_min_date_height(today).height
        return self.db_client.get_restake_execution_count_actual(height_from).value

    def get_whale_transactions(self, limit, offset):
        week_ago = str(date.today() - timedelta(days=7))
        height_from = self.db_client.get_min_date_height(week_ago).height
        whale_transactions = self.db_client.get_whale_transactions(limit, offset, height_from)
        tx_hashes = [item.tx_hash for item in whale_transactions]
        transactions_details = self.db_client.get_whale_transaction_details(tx_hashes)
        result = []
        for item in whale_transactions:
            details = next((detail_item for detail_item in transactions_details if detail_item.tx_hash == item.tx_hash), None)
            if details:
                info_to_add = details._asdict()
                info_to_add['details'] = json.loads(info_to_add['details'])
            else:
                info_to_add = {
                    'details': {},
                    'type': ''
                }
            result.append({**item._asdict(), **info_to_add})
        return result
