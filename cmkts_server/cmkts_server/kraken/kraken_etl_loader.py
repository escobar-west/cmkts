from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from cmkts_server.etl_loader import EtlLoader
from cmkts_server.kraken.kraken_api_handler import KrakenApiHandler


class KrakenEtlLoader(EtlLoader):
    def __init__(
        self,
        start: str | None = None,
        end: str | None = None,
        tz: str = "America/New_York",
    ) -> None:
        self._tz = ZoneInfo(tz)
        if start is not None:
            start_date = datetime.strptime(start, "%Y-%m-%d")
            self._start_date = start_date.replace(tzinfo=self._tz)
        else:
            self._start_date = datetime.fromtimestamp(0, tz=self._tz)
        if end is not None:
            end_date = datetime.strptime(end, "%Y-%m-%d")
            self._end_date = end_date.replace(tzinfo=self._tz)
        else:
            self._end_date = datetime.now(tz=self._tz)
        

    def extract(self) -> pl.DataFrame:
        start_epoch_nano = int(self._start_date.timestamp()) * int(1e9)
        end_epoch_nano = int(self._end_date.timestamp()) * int(1e9)
        api_handler = KrakenApiHandler()
        batches = []
        while start_epoch_nano < end_epoch_nano:
            data = api_handler.get_recent_trades("ETHUSD", start_epoch_nano)
            print(f"getting batch {datetime.fromtimestamp(start_epoch_nano // int(1e9), tz=self._tz)}")
            last_time = int(data["result"]["last"])
            if last_time == start_epoch_nano:
                break
            start_epoch_nano = last_time
            batches.append(data["result"]["XETHZUSD"])
        rows = [row for batch in batches for row in batch]
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
        ).unique(subset="id")
        return df

    def transform(self):
        pass

    def load(self):
        pass
