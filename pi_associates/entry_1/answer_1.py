from pi_associates_library.config_loader import EnvironConfigLoader
from pi_associates_library.job_runner import ThreadJobRunner, JobRunner
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnTickCSVFile, KisvnTickStorage
from pi_associates_library.kisvn.kisvn_scraper import KisvnTickScraper
from datetime import datetime
import logging
import os.path


if __name__ == '__main__':
    job_runner: JobRunner = ThreadJobRunner()
    config: dict = EnvironConfigLoader().load_config()

    raw_tickdata_dirpath = config.get('PI_ASSOCIATES_KISVN_RAW_DATA_DIRPATH')
    log_dirpath = config.get('PI_ASSOCIATES_KISVN_LOG_DATA_DIRPATH')
    vn30_symbols = config.get('PI_ASSOCIATES_VN30_SYMBOLS').split(',')

    log_filepath = datetime.now().strftime("Ent1_Ans1_%Y%b%d_%H%M%S") + '.log'
    logging.basicConfig(filename=os.path.join(log_dirpath, log_filepath), level=logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler())

    for symbol in [*vn30_symbols, 'VN30']:
        tick_storage : KisvnTickStorage = KisvnTickCSVFile(stock=symbol,
                                                           data_dirpath=raw_tickdata_dirpath)
        tick_scraper = KisvnTickScraper(symbol=symbol,
                                        tick_storage=tick_storage)

        job_runner.create_job(job_name=symbol,
                              target=tick_scraper.run_in_one_day, args=[])

        logging.info(f'[GLOBAL] create JOB for {symbol}')

    job_runner.run_until_complete()
