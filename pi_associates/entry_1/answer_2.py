from datetime import date, timedelta
import pandas as pd

from pi_associates_library.kisvn.kisvn_persistent import KisvnTickCSVFile, KisvnTickStorage, \
    KisvnProcessedTickPersistent, KisvnProcessedTickCSVFile
from pi_associates_library.job_runner import ProcessJobRunner, ThreadJobRunner, JobRunner
from pi_associates_library.config_loader import DotenvConfigLoader
from datetime import datetime


class KisvnTickProcessor:
    def __init__(self, source: KisvnTickStorage, dest: KisvnProcessedTickPersistent):
        self.source = source
        self.dest = dest

    def process(self, date: datetime):
        pass


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('expand_frame_repr', False)

    param_today = datetime.now() - timedelta(days=1)

    job_runner: JobRunner = ThreadJobRunner()
    config: dict = DotenvConfigLoader().load_config()

    raw_dirpath = config.get('PI_ASSOCIATES_KISVN_RAW_DATA_DIRPATH')
    processed_dirpath = config.get('PI_ASSOCIATES_KISVN_PROCESSED_DATA_DIRPATH')
    symbol_list = config.get('PI_ASSOCIATES_VNINDEX_SYMBOLS').split(',')

    for symbol in symbol_list:
        print(f'stock = {symbol}')
        tick_storage: KisvnTickStorage = KisvnTickCSVFile(stock=symbol,
                                                          base_dirpath=raw_dirpath)
        processed_tick_storage: KisvnProcessedTickPersistent = KisvnProcessedTickCSVFile(stock=symbol,
                                                                                         data_dirpath=raw_dirpath)

        df0 = tick_storage.select_all(date=param_today)
        df1: pd.DataFrame = df0.drop_duplicates(subset=None,
                                                keep='first',
                                                inplace=False,
                                                ignore_index=False)
        # print(df1.info())
        # print(df1)
        # print(df1.index)
        print(f"\n{'*'*20}\ndf2")
        # df2: pd.DataFrame = df1[['t', 'mv', 'vo']]
        df2 = df1
        df2 = df2.sort_values(by=['vo'], ascending=False, ignore_index=False)

        print(df2.info())
        print(df2)
        print(f"\n{'*' * 20}\ndf2")

        df2 = df2[['t', 'vo', 'mv']]
        df2['calculate_vo'] = df2.apply(lambda row: row.vo + row.mv, axis=1)
        df2['t'] = df2.apply(lambda row: datetime.strptime(row.t, "%H%M%S").time(), axis=1)
        print(df2['vo'].is_monotonic_increasing)

        df3 = pd.merge(df2, df2,
                       how='outer',
                       left_on='calculate_vo', right_on='vo')
        # %H%M%S
        print(f"\n{'*'*20}\ndf3")
        print(df3)
        exit(0)
