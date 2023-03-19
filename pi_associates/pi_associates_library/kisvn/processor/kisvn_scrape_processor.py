from pi_associates_library.job_runner import ThreadJobRunner, JobRunner
from pi_associates_library.kisvn.kisvn_scraper_executor import KisvnTickCacheScraper, KisvnTickScraper
from pi_associates_library.kisvn.kisvn_scraper_delay_strategy import KisvnScraperDynamicDelay
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnTickDateStorage, KisvnRawTickCSVFile
from pi_associates_library.kisvn.kisvn_scraper_runner import KisvnTickScraperRunner
from pi_associates_library.kisvn.processor.kisvn_processor import KisvnProcessor
from pi_associates_library.vnindex.trade_time import HOSETradeTimeRange
from datetime import datetime


class KisvnScrapeProcessor(KisvnProcessor):
    def execute(self):
        job_runner: JobRunner = ThreadJobRunner()
        now = datetime.today()
        trade_timeranges = HOSETradeTimeRange().get_timeranges(now)

        self.logger.info(f"Scraper symbols: {', '.join(self.params.symbols())}")
        self.logger.info(f'Scraper Raw dir: {self.params.raw_datadir()}')
        self.logger.info(f'Trade times of {now.today().date()} are {trade_timeranges}')
        for symbol in self.params.symbols():
            storage : KisvnTickDateStorage = KisvnRawTickCSVFile(symbol=symbol,
                                                                 datadir=self.params.raw_datadir(),
                                                                 trade_date=now)
            scraper: KisvnTickScraper = KisvnTickCacheScraper(symbol)
            scraper_runner = KisvnTickScraperRunner(symbol=symbol,
                                                    storage=storage,
                                                    scraper=scraper,
                                                    delay_strategy=KisvnScraperDynamicDelay())

            job_runner.create_job(job_name=symbol,
                                  target=scraper_runner.run_in_timeranges, args=[trade_timeranges])

        job_runner.run_until_complete()
