from pi_associates_library.kisvn.kisvn_url import KisvnEndpoint
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnTickStorage
from datetime import datetime
from tenacity import retry, stop_after_attempt
import pandas as pd
import requests
import sched
import time
import logging


def _vnindex_scrape_timeranges(day: datetime):
    begin1 = day.replace(hour=9, minute=10, second=0, microsecond=0)
    end1 = day.replace(hour=11, minute=35, second=0, microsecond=0)

    begin2 = day.replace(hour=12, minute=55, second=0, microsecond=0)
    end2 = day.replace(hour=14, minute=50, second=0, microsecond=0)

    return [(begin1, end1), (begin2, end2)]


class KisvnTickScraper:
    def __init__(self, symbol, tick_storage: KisvnTickStorage):
        self.symbol = symbol
        self.tick_storage = tick_storage
        self.logger = logging.getLogger(symbol)

        self.__scrape_datetime = datetime.now()
        self.__http_session = None
        self.__df0 = self.__df1 = None

    def __http_request_tickdata(self):
        response = self.__http_session.get(KisvnEndpoint.REALTIME_TICKS(self.symbol))
        response = response.json()
        return pd.DataFrame(response)

    @retry(stop=stop_after_attempt(3))
    def run_in_timerange(self, begin: datetime, end: datetime):
        def get_left_anti_join_dataframe(_df1: pd.DataFrame, _df0: pd.DataFrame):
            if _df0 is None:
                return _df1

            _df = pd.merge(_df1, _df0, how='left', indicator=True).query('_merge=="left_only"')
            _df = _df.drop(['_merge'], axis=1)
            return _df

        self.logger.info(f'[RUN_IN_TIMERANGE] now is {datetime.now().time()}, task runs from {begin.time()} to {end.time()}')

        count = 0
        while begin < datetime.now() < end or count < 1:
            count += 1
            self.__df1 = self.__http_request_tickdata()
            self.logger.info(f'[RUN_IN_TIMERANGE::SCRAPE] dataframe has row-numbers={len(self.__df1.index)}')
            if self.__df1.empty:
                delay = 10
            else:
                df = get_left_anti_join_dataframe(self.__df1, self.__df0)

                if not df.empty:
                    self.tick_storage.add_tickdata(df, trade_date=self.__scrape_datetime)

                delay = 40 - len(df.index)*30/KisvnEndpoint.MAX_REALTIME_TICKS_NUMBER_PER_REQUEST
                self.logger.info(f'[RUN_IN_TIMERANGE::COMPARE] new dataframe has row-numbers={len(df.index)}, '
                                 f'delay {delay} sec')

            self.__df0 = self.__df1

            time.sleep(delay)

    @retry(stop=stop_after_attempt(10))
    def run_in_one_day(self):
        self.__df0 = self.__df1 = None
        self.__http_session = requests.session()

        scheduler = sched.scheduler(time.time, time.sleep)
        scrape_timeranges = _vnindex_scrape_timeranges(self.__scrape_datetime)

        for begin, end in scrape_timeranges:
            scheduler.enterabs(time=begin.timestamp(),
                               priority=1,
                               action=self.run_in_timerange, argument=(begin, end))

        scheduler.run(blocking=True)

        self.__http_session.close()
