from pi_associates_library.kisvn.kisvn_tick_processor import KisvnTickProcessor
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnTickCSVFile, KisvnTickStorage
from pi_associates_library.config_loader import EnvironConfigLoader
from datetime import datetime
import pandas as pd


def print_dataframe(df: pd.DataFrame, header):
    print(f'=============={header}=================')
    print(f'HASH is {df.__hash__}')
    print(df.info())
    print(f'index is mono increase {df.index.is_monotonic_increasing}')
    print(df)
    print()


if __name__ == '__main__':

    config: dict = EnvironConfigLoader().load_config()

    tick_csv_data_dirpath = config.get('PI_ASSOCIATES_KISVN_PROCESSED_DATA_DIRPATH')
    vn30_symbols = config.get('PI_ASSOCIATES_VN30_SYMBOLS').split(',')

    param_today = datetime.today()
    for symbol in vn30_symbols:
        tick_storage: KisvnTickStorage = KisvnTickCSVFile(stock=symbol, data_dirpath=tick_csv_data_dirpath)
        df0 = tick_storage.read_tickdata(trade_date=param_today)

        print_dataframe(df0, 'processed')
        df1 = KisvnTickProcessor.get_missing_tickdata(df0)
        print_dataframe(df1, 'result')
        exit(1)