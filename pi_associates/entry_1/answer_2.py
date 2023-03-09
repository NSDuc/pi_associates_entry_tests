from pi_associates_library.kisvn.kisvn_tick_processor import KisvnTickProcessor
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnTickCSVFile, KisvnTickStorage
from pi_associates_library.config_loader import EnvironConfigLoader
from datetime import datetime
import pandas as pd


def init_pandas():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('expand_frame_repr', False)


if __name__ == '__main__':
    param_trade_dates = [
        datetime(year=2023, month=3, day=13)
    ]

    config: dict = EnvironConfigLoader().load_config()

    raw_dirpath = config.get('PI_ASSOCIATES_KISVN_RAW_DATA_DIRPATH')
    prc_dirpath = config.get('PI_ASSOCIATES_KISVN_PROCESSED_DATA_DIRPATH')
    vn30_symbols = config.get('PI_ASSOCIATES_VN30_SYMBOLS').split(',')


    for trade_date in param_trade_dates:
        for symbol in vn30_symbols:
            src_tick_storage: KisvnTickStorage = KisvnTickCSVFile(stock=symbol, data_dirpath=raw_dirpath)
            dst_tick_storage: KisvnTickStorage = KisvnTickCSVFile(stock=symbol, data_dirpath=prc_dirpath)

            df0 = src_tick_storage.read_tickdata(trade_date=trade_date)

            df1 = KisvnTickProcessor.clean_raw_tickdata(df0)

            dst_tick_storage.overwrite_tickdata(df1, trade_date=trade_date)
