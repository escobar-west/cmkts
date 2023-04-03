from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from cmkts_server.etl_loader import EtlLoader
from cmkts_server.kraken.kraken_api_handler import KrakenApiHandler


class KrakenEtlLoader(EtlLoader):
    def __init__(self, start: str | None = None, tz: str = "America/New_York") -> None:
        self._tz = ZoneInfo(tz)
        if start is not None:
            start_date = datetime.strptime(start, "%Y-%m-%d")
            self._start_date = start_date.replace(tzinfo=self._tz)
        else:
            self._start_date = datetime.fromtimestamp(0, tz=self._tz)

    def extract(self) -> pl.DataFrame:
        epoch = int(self._start_date.timestamp())
        api_handler = KrakenApiHandler()
        data = api_handler.get_recent_trades("ETHUSD", epoch)
        rows = data["result"]["XETHZUSD"]
        df = pl.DataFrame(
            rows,
            schema=[
                ("price", pl.Utf8),
                ("volume", pl.Utf8),
                ("time", pl.Float32),
                ("side", pl.Categorical),
                ("order", pl.Categorical),
                ("misc", pl.Utf8),
                ("id", pl.UInt32),
            ],
        )
        return df

    def transform(self):
        pass

    def load(self):
        pass
