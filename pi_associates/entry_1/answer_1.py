from pi_associates_library.config_loader import EnvironConfigLoader
from pi_associates_library.job_runner import ProcessJobRunner, ThreadJobRunner, JobRunner
from pi_associates_library.kisvn.kisvn_persistent import KisvnTickCSVFile, KisvnTickStorage
from pi_associates_library.kisvn.kisvn_scraper import KisvnTickScraper
from datetime import datetime
import logging
import os.path


if __name__ == '__main__':
    job_runner: JobRunner = ThreadJobRunner()
    config: dict = EnvironConfigLoader().load_config()

    base_dirpath = config.get('PI_ASSOCIATES_KISVN_RAW_DATA_DIRPATH')
    log_dirpath = config.get('PI_ASSOCIATES_KISVN_LOG_DIRPATH')
    symbol_list = config.get('PI_ASSOCIATES_VNINDEX_SYMBOLS').split(',')

    now = datetime.now().strftime("%H-%M-%S")
    logging.basicConfig(filename=os.path.join(log_dirpath, f'{now}.log'), level=logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler())

    for symbol in symbol_list:
        tick_storage : KisvnTickStorage = KisvnTickCSVFile(stock=symbol,
                                                           base_dirpath=base_dirpath)
        tick_scraper = KisvnTickScraper(symbol=symbol,
                                        tick_storage=tick_storage)

        job_runner.create_job(job_name=symbol,
                              target=tick_scraper.run_in_one_day, args=[])

        logging.info(f'[GLOBAL] create JOB for {symbol}')

    job_runner.run_until_complete()
