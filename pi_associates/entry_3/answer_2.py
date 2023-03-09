from pi_associates_library.config_loader import EnvironConfigLoader
from pi_associates_library.job_runner import ThreadJobRunner, JobRunner
from pi_associates_library.vps.vps_scraper import VPSDataStorage, VPSWebSocketScraper
from datetime import datetime
import logging
import os.path


if __name__ == '__main__':
    job_runner: JobRunner = ThreadJobRunner()
    config: dict = EnvironConfigLoader().load_config()

    base_dirpath = config.get('PI_ASSOCIATES_VPS_RAW_DATA_DIRPATH')
    log_dirpath = config.get('PI_ASSOCIATES_VPS_LOG_DATA_DIRPATH')
    vn30_symbols = config.get('PI_ASSOCIATES_VN30_SYMBOLS').split(',')

    log_filepath = datetime.now().strftime("Ent3_Ans2_%Y%b%d_%H%M%S") + '.log'
    logging.basicConfig(filename=os.path.join(log_dirpath, log_filepath), level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())

    handler = VPSDataStorage(vn30_symbols)
    scraper = VPSWebSocketScraper(symbol_list=vn30_symbols,
                                  vps_websocket_data_handler=handler)
    scraper.run_forever()
