from pi_associates_library.kisvn.kisvn_url import KisvnEndpoint
from pi_associates_library.kisvn.kisvn_persistent import KisvnTickStorage
from datetime import datetime
from tenacity import retry, stop_after_attempt
import pandas as pd
import requests
import sched
import time
import logging


def _vnindex_scrape_timeranges(day: datetime):
    begin1 = day.replace(hour=9, minute=14, second=0, microsecond=0)
    end1 = day.replace(hour=11, minute=31, second=0, microsecond=0)

    begin2 = day.replace(hour=12, minute=59, second=0, microsecond=0)
    end2 = day.replace(hour=14, minute=47, second=0, microsecond=0)

    return [(begin1, end1), (begin2, end2)]


class KisvnTickScraper:
    def __init__(self, symbol, tick_storage: KisvnTickStorage):
        self.symbol = symbol
        self.tick_storage = tick_storage
        self.logger = logging.getLogger(symbol)

        self.__scrape_datetime = datetime.now()
        self.__http_session = None
        self.__df0 = self.__df1 = None

    @retry(stop=stop_after_attempt(3))
    def scrape(self):
        response = self.__http_session.get(KisvnEndpoint.REALTIME_TICKS(self.symbol))
        response = response.json()
        return pd.DataFrame(response)

    @retry(stop=stop_after_attempt(10))
    def run_in_timerange(self, begin: datetime, end: datetime):
        self.logger.info(f'[RUN_IN_TIMERANGE] now is {datetime.now().time()}, task runs from {begin.time()} to {end.time()}')

        count = 0
        while begin < datetime.now() < end or count < 1:
            count += 1
            self.__df1 = self.scrape()
            self.logger.info(f'[RUN_IN_TIMERANGE::SCRAPE] dataframe has row-numbers={len(self.__df1.index)}')
            if self.__df1.empty:
                delay = 15
            else:
                df = pd.concat([self.__df1, self.__df0]).drop_duplicates(keep=False)
                self.logger.info(f'[RUN_IN_TIMERANGE::COMPARE] new dataframe has row-numbers={len(df.index)}')
                self.logger.info(f'[RUN_IN_TIMERANGE] new dataframe info: {df.tail()}')
                if not df.empty:
                    self.tick_storage.insert(df, date=self.__scrape_datetime)

                delay = 45 - len(df.index)*30/100

            self.logger.info(f'[RUN_IN_TIMERANGE][DELAY] {delay} seconds')
            self.__df0 = self.__df1

            time.sleep(delay)


    @retry(stop=stop_after_attempt(3))
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
