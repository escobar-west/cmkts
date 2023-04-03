import requests


class KrakenApiHandler:
    def __init__(self) -> None:
        self._session = requests.Session()

    def get_recent_trades(self, pair: str, epoch_start: int) -> dict:
        endpoint = f"/public/Trades?pair={pair}&since={epoch_start}"
        url = self.root_url + endpoint
        resp = self._session.get(url)
        return resp.json()

    @property
    def root_url(self) -> str:
        return "https://api.kraken.com/0"
