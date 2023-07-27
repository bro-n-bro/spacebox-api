import asyncio

import aiohttp
import requests

from common.constants import OSMO_LOGO_URL
from common.decorators import response_decorator
from config.config import LCD_API, PRICE_FEED_API
from typing import Optional, Tuple, List
from urllib.parse import urljoin


class BronbroApiClient:

    def __init__(self):
        self.lcd_api_url = LCD_API
        self.price_feed_api_url = PRICE_FEED_API

    @response_decorator
    def lcd_get(self, url):
        url = urljoin(self.lcd_api_url, url)
        return requests.get(url)

    @response_decorator
    def rpc_get(self, url):
        url = urljoin(self.price_feed_api_url, url)
        return requests.get(url)

    def get_address_rewards(self, address: str) -> Optional[dict]:
        return self.lcd_get(f'/cosmos/distribution/v1beta1/delegators/{address}/rewards')

    def get_annual_provisions(self) -> Optional[dict]:
        return self.lcd_get('cosmos/mint/v1beta1/annual_provisions')

    def get_exchange_rates(self) -> List[dict]:
        return self.rpc_get('price_feed_api/tokens/')

    def get_token_logo(self, token: str) -> dict:
        return self.rpc_get(f'skychart/v1/asset/{token}')

    def get_slash_params(self) -> dict:
        return self.lcd_get(f'cosmos/slashing/v1beta1/params')

    async def get_symbol_from_denom(self, session, denom: str) -> dict:
        url = urljoin(self.lcd_api_url, f'ibc/apps/transfer/v1/denom_traces/{denom.split("/")[1]}')
        async with session.get(url) as resp:
            response = await resp.json()
            return {
                'denom': denom,
                'symbol': response.get('denom_trace').get('base_denom')
            }

    async def get_logo_for_symbol(self, session, symbol: str) -> dict:
        url = urljoin(self.price_feed_api_url, f'skychart/v1/asset/{symbol}')
        async with session.get(url) as resp:
            response = await resp.json()
            logo = response.get('logo_URIs', {}).get('svg', '') if resp.ok else ''
            if not logo and symbol == 'osmo':
                logo = OSMO_LOGO_URL
            return {
                'symbol': symbol,
                'logo': logo
            }

    async def get_symbols_from_denoms(self, denoms: List[str]):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for denom in denoms:
                tasks.append(asyncio.ensure_future(self.get_symbol_from_denom(session, denom)))

            return await asyncio.gather(*tasks)

    async def get_symbols_logos(self, symbols: List[str]):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for symbol in symbols:
                tasks.append(asyncio.ensure_future(self.get_logo_for_symbol(session, symbol)))

            return await asyncio.gather(*tasks)

    async def get_balance_item(self, session, item_info: dict) -> dict:
        async with session.get(item_info.get('endpoint')) as resp:
            response = await resp.json()
            return {
                'response': response,
                'type': item_info.get('type')
            }

    async def get_account_balances(self, address):
        balance_items_to_receive = [
            {
                "endpoint": f"{LCD_API}/cosmos/bank/v1beta1/balances/{address}?pagination.limit=1000",
                "type": "liquid"
            },
            {
                "endpoint": f"{LCD_API}/cosmos/staking/v1beta1/delegations/{address}",
                "type": "staked"
            },
            {
                "endpoint": f"{LCD_API}/cosmos/staking/v1beta1/delegators/{address}/unbonding_delegations",
                "type": "unbonding"
            },
            {
                "endpoint": f"{LCD_API}/cosmos/distribution/v1beta1/delegators/{address}/rewards",
                "type": "rewards"
            }
        ]
        async with aiohttp.ClientSession() as session:
            tasks = []
            for balance_item in balance_items_to_receive:
                tasks.append(asyncio.ensure_future(self.get_balance_item(session, balance_item)))
            return await asyncio.gather(*tasks)
