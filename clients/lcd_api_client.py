import requests

from common.decorators import response_decorator
from config.config import LCD_API
from typing import Optional, Tuple
from urllib.parse import urljoin


class LcdApiClient:

    def __init__(self):
        self.api_url = LCD_API

    @response_decorator
    def get(self, url):
        url = urljoin(self.api_url, url)
        return requests.get(url)

    def get_address_rewards(self, address: str) -> Optional[dict]:
        return self.get(f'/cosmos/distribution/v1beta1/delegators/{address}/rewards')

    def get_annual_provisions(self) -> Optional[dict]:
        return self.get('cosmos/mint/v1beta1/annual_provisions')
