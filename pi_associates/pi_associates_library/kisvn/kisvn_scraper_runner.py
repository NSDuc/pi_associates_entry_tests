from pi_associates_library.kisvn.kisvn_scraper_delay_strategy import KisvnScraperDelayStrategy
from pi_associates_library.kisvn.kisvn_scraper_executor import KisvnTickScraper
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnTickDateStorage
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt
import sched
import time
import logging


class KisvnTickScraperRunner:
    def __init__(self, symbol,
                 storage: KisvnTickDateStorage,
                 scraper: KisvnTickScraper,
                 delay_strategy: KisvnScraperDelayStrategy):
        self.logger = logging.getLogger(f'{self.__class__.__name__}.{symbol}')

        self.symbol = symbol
        self.tick_storage = storage
        self.tick_scraper = scraper
        self.delay_strategy = delay_strategy

    @retry(stop=stop_after_attempt(50))
    def run_in_timerange(self, begin: datetime, end: datetime):
        self.logger.info(f'Start for timerange {begin.time()} to {end.time()} (at {datetime.now().time()})')
        counter = 0
        while begin < datetime.now() < end or counter < 1:
            counter += 1

            tickdata = self.tick_scraper.get_tickdata()

            self.tick_storage.insert_into(tickdata)

            delay = self.delay_strategy.calculate_delay(tickdata)

            self.logger.info(f"GET {len(tickdata.index):3} tickdata, delay {delay}s")

            time.sleep(delay)

        self.logger.info(f"End for timerange {begin.time()} to {end.time()}, number of request is {counter}")

    def run_in_timeranges(self, timeranges):
        scheduler = sched.scheduler(time.time, time.sleep)

        for begin, end in timeranges:
            # if now > end:
            #     self.logger.warning(f"Skip trade-time from {begin} to {end}")
            #     continue

            # run early and stop late 3 max-delay times
            _delta = 3 * self.delay_strategy.maximum_delay_in_seconds()
            _begin = begin - timedelta(seconds=_delta)
            _end = end + timedelta(seconds=_delta)

            self.logger.info(f'register task timerange {_begin.time()} to {_end.time()} (at {datetime.now().time()})')
            scheduler.enterabs(time=_begin.timestamp(),
                               priority=1,
                               action=self.run_in_timerange, argument=(_begin, _end))

        scheduler.run(blocking=True)
