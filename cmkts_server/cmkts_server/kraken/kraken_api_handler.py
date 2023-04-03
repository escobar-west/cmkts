import requests
import datetime as dt
import polars


class KrakenApiHandler:
    def __init__(self):
        self._session = requests.Session()

    def get_recent_trades(self, pair: str, epoch_start: int):
        endpoint = f"/public/Trades?pair={pair}&since={epoch_start}"
        url = self.root_url + endpoint
        resp = self._session.get(url)
        print(resp.json())

    @property
    def root_url(self):
        return "https://api.kraken.com/0"
