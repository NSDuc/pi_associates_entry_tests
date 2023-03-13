from pi_associates_library.config_loader import DotenvConfigLoader
from pi_associates_library.job_runner import JobRunner, ThreadJobRunner
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnTickStorage, KisvnTickCSVFile
from pi_associates_library.kisvn.kisvn_scraper import KisvnTickScraper
from unittest import TestCase


class TestAnswer1(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestAnswer1, self).__init__(*args, **kwargs)
        self.job_runner: JobRunner = ThreadJobRunner()
        self.config: dict = DotenvConfigLoader().load_config('.env.test')

    def test(self):
        dirpath = self.config.get('PI_ASSOCIATES_KISVN_RAW_DATA_DIRPATH')
        symbols = self.config.get('PI_ASSOCIATES_VNINDEX_SYMBOLS').split(',')

        for symbol in symbols:
            if symbol != 'NVL':
                continue
            tick_storage: KisvnTickStorage = KisvnTickCSVFile(stock=symbol, data_dirpath=dirpath)
            tick_scraper = KisvnTickScraper(symbol=symbol, tick_storage=tick_storage)

            self.job_runner.create_job(job_name=symbol,
                                       target=tick_scraper.run_in_one_day, args=[])

        self.job_runner.run_all()
        self.job_runner.wait_all()
