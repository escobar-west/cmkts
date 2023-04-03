from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo
from cmkts_server.etl_loader import EtlLoader
from cmkts_server.kraken.kraken_api_handler import KrakenApiHandler


class KrakenEtlLoader(EtlLoader):
    def __init__(self, start: Optional[str], tz: str = "America/New_York"):
        self._api_handler = KrakenApiHandler()
        self._tz = ZoneInfo(tz)
        if start is not None:
            start = datetime.strptime(start, "%Y-%m-%d")
            self._start_date = start.replace(tzinfo=self._tz)
        else:
            self._start_date = datetime.fromtimestamp(0, tz=self._tz)

    def extract(self):
        epoch = int(self._start_date.timestamp())
        self._api_handler.get_recent_trades("XBTUSD", epoch)

    def transform(self):
        pass

    def load(self):
        pass
