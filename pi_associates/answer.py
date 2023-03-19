from pi_associates_library.kisvn.processor.kisvn_processor import KisvnProcessorParams
from pi_associates_library.kisvn.processor.kisvn_processor_factory import KisvnProcessorFactory
from pi_associates_library.config_loader import EnvironConfigLoader
from datetime import datetime, date
import argparse
import logging
import os


def init_logging(filename=None, filename_prefix=None, filedir=None,
                 loglevel=logging.INFO, fmt=None, stream=True):
    formatter = logging.Formatter(fmt if fmt
                                  else '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                                  "%H:%M:%S")
    if stream:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
    if (filename or filename_prefix) and filedir:
        filename = filename or datetime.now().strftime(f"{filename_prefix}_%Y%b%d_%H%M%S.log")

        handler = logging.FileHandler(os.path.join(filedir, filename))
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(loglevel)


if __name__ == '__main__':
    # load OS environment variable for default config
    environ = EnvironConfigLoader().load_config()
    VN30_EXT_SYMBOLS = environ.get('PI_ASSOCIATES_VN30_EXTENSION_SYMBOLS').split(',')
    VN30INDEX_SYMBOL = environ.get('PI_ASSOCIATES_VN30INDEX_SYMBOL')
    VN30_SYMBOLS = [s for s in VN30_EXT_SYMBOLS if s != VN30INDEX_SYMBOL]

    # parse arguments
    parser = argparse.ArgumentParser(description='Pi-Associates answers',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--process-name', required=True,
                        choices=['scrape', 'raw', 'missing', 'liquid', 'liquid-for-missing'],
                        help="Process task")

    parser.add_argument('--process-dates', default=date.today().strftime('%d-%m-%Y'),
                        help="Process date(s) in format %%d-%%m-%%Y, split by comma. Default is today")

    parser.add_argument('--process-symbols', default=environ['PI_ASSOCIATES_VN30_EXTENSION_SYMBOLS'],
                        help="Process stock symbol(s), split by comma")

    parser.add_argument('--log-level', default='INFO',
                        choices=['INFO', 'DEBUG', 'WARNING', 'ERROR'],
                        help="Log level")
    parser.add_argument('--log-dir', default=environ['PI_ASSOCIATES_KISVN_LOG_DIR'],
                        help="Log directory")

    # Kisvn Tick Data Directories Arguments
    parser.add_argument('--raw-data-dir', default=environ['PI_ASSOCIATES_KISVN_TICKDATA_DIR_RAW'],
                        help="Raw tick data directory")

    parser.add_argument('--processed-data-dir', default=environ['PI_ASSOCIATES_KISVN_TICKDATA_DIR_PROCESSED'],
                        help="Processed tick data directory")

    args = parser.parse_args()

    # setup logging
    init_logging(filename_prefix=args.process_name, filedir=args.log_dir, loglevel=args.log_level)

    process_params = KisvnProcessorParams(
        symbols=args.process_symbols.split(','),
        process_dates=[datetime.strptime(s, "%d-%m-%Y").date() for s in args.process_dates.split(',')],
        raw_datadir=args.raw_data_dir,
        processed_datadir=args.processed_data_dir,
    )

    # Each '--process-name' is corresponding with a processor
    processor = KisvnProcessorFactory.create_processor(name=args.process_name,
                                                       params=process_params)
    processor.execute()
