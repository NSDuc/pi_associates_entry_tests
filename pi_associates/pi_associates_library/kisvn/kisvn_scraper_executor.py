from pi_associates_library.kisvn.kisvn_url import KisvnHttpURL
from typing import Optional
import pandas as pd
import requests


class KisvnTickScraper:
    def __init__(self, symbol):
        self.symbol: str = symbol
        self.http_session: Optional[requests.Session] = None
        self.http_url = KisvnHttpURL.REALTIME_TICKS(self.symbol)

    def get_tickdata(self):
        raise NotImplementedError


class KisvnTickDummyScraper(KisvnTickScraper):
    def get_tickdata(self):
        response = self.http_session.get(self.http_url)
        response = response.json()
        return pd.DataFrame(response)


class KisvnTickCacheScraper(KisvnTickScraper):
    def __init__(self, symbol):
        super().__init__(symbol)
        self.cached_tickdata : Optional[pd.DataFrame] = None

    def get_tickdata(self):
        try:
            if self.http_session is None:
                self.http_session = requests.session()

            response = self.http_session.get(self.http_url)
            response = response.json()
            tickdata = pd.DataFrame(response)

            diff_tickdata = self.compare_to_cached(tickdata)

            self.cached_tickdata = tickdata
        except Exception as e:
            self.http_session = None
            return None
        return diff_tickdata

    def compare_to_cached(self, tickdata: pd.DataFrame) -> pd.DataFrame:
        if self.cached_tickdata is None:
            return tickdata
        elif self.cached_tickdata.empty:
            return tickdata

        _df = pd.merge(tickdata, self.cached_tickdata, how='left', indicator=True).query('_merge=="left_only"')
        _df.drop(['_merge'], axis=1, inplace=True)
        return _df
