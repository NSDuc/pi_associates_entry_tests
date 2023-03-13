from pi_associates_library.kisvn.kisvn_tick_processor import KisvnTickProcessor
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnTickCSVFile, KisvnTickStorage
from pi_associates_library.config_loader import EnvironConfigLoader
from datetime import datetime
import pandas as pd


def init_pandas():
    pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)
    pd.set_option('expand_frame_repr', False)


def print_dataframe(df: pd.DataFrame, header):
    print(f'=============={header}=================')
    print(df.info())
    print(f'index is mono increase {df.index.is_monotonic_increasing}')
    print(df)
    print()


if __name__ == '__main__':
    init_pandas()

    config: dict = EnvironConfigLoader().load_config()

    raw_dirpath = config.get('PI_ASSOCIATES_KISVN_RAW_DATA_DIRPATH')
    prc_dirpath = config.get('PI_ASSOCIATES_KISVN_PROCESSED_DATA_DIRPATH')
    vn30_symbols = config.get('PI_ASSOCIATES_VN30_SYMBOLS').split(',')

    param_today = datetime.today()
    for symbol in vn30_symbols:
        src_tick_storage: KisvnTickStorage = KisvnTickCSVFile(stock=symbol, data_dirpath=raw_dirpath)
        dst_tick_storage: KisvnTickStorage = KisvnTickCSVFile(stock=symbol, data_dirpath=prc_dirpath)

        df0 = src_tick_storage.read_tickdata(trade_date=param_today)

        df1 = KisvnTickProcessor.clean_raw_tickdata(df0)

        dst_tick_storage.overwrite_tickdata(df1, trade_date=param_today)

        # df3 = df1[['t', 'vo', 'mv']]
        # df3['final_vo'] = df1.apply(lambda row: row.vo + row.mv, axis=1)
        # df1['t'] = df1.apply(lambda row: datetime.strptime(row.t, "%H%M%S").time(), axis=1)
        #
        # df3 = pd.merge(df1, df1,
        #                how='outer',
        #                left_on='final_vo', right_on='vo')
        # # %H%M%S
        # print(f"\n{'*'*20}\ndf3")
        # print(df3)
        # exit(0)
