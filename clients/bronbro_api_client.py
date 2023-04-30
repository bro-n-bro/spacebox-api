import asyncio

import aiohttp
import requests

from common.decorators import response_decorator
from config.config import LCD_API, RPC_API
from typing import Optional, Tuple, List
from urllib.parse import urljoin


class BronbroApiClient:

    def __init__(self):
        self.lcd_api_url = LCD_API
        self.rpc_api_url = RPC_API

    @response_decorator
    def lcd_get(self, url):
        url = urljoin(self.lcd_api_url, url)
        return requests.get(url)

    @response_decorator
    def rpc_get(self, url):
        url = urljoin(self.rpc_api_url, url)
        return requests.get(url)

    def get_address_rewards(self, address: str) -> Optional[dict]:
        return self.lcd_get(f'/cosmos/distribution/v1beta1/delegators/{address}/rewards')

    def get_annual_provisions(self) -> Optional[dict]:
        return self.lcd_get('cosmos/mint/v1beta1/annual_provisions')

    def get_exchange_rates(self) -> List[dict]:
        return self.rpc_get('price_feed_api/tokens/')

    async def get_symbol_from_denom(self, session, denom: str) -> dict:
        url = urljoin(self.lcd_api_url, f'ibc/apps/transfer/v1/denom_traces/{denom.split("/")[1]}')
        async with session.get(url) as resp:
            response = await resp.json()
            return {
                'denom': denom,
                'symbol': response.get('denom_trace').get('base_denom')
            }

    async def get_symbols_from_denoms(self, denoms: List[str]):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for denom in denoms:
                tasks.append(asyncio.ensure_future(self.get_symbol_from_denom(session, denom)))

            return await asyncio.gather(*tasks)
