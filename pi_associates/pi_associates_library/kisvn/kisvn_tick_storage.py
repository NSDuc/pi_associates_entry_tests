from typing import Optional
from datetime import date
import os.path
import pandas as pd


class KisvnTickDateStorage:
    def __init__(self, symbol):
        self.symbol = symbol

    def insert_into(self, df: Optional[pd.DataFrame], **kwargs):
        raise NotImplementedError

    def truncate(self, df: pd.DataFrame, **kwargs):
        raise NotImplementedError

    def recreate_storage(self, df: pd.DataFrame, **kwargs):
        raise NotImplementedError

    def select_all(self, **kwargs) -> pd.DataFrame:
        raise NotImplementedError


class KisvnRawTickCSVFile(KisvnTickDateStorage):
    def __init__(self, symbol, datadir, trade_date: date):
        super().__init__(symbol)
        self.filedir = os.path.join(datadir, trade_date.strftime("%d%m%Y"))
        self.filepath = os.path.join(self.filedir, f'{symbol}.csv')
        os.makedirs(self.filedir, exist_ok=True)

    def insert_into(self, df: Optional[pd.DataFrame], **kwargs):
        if (df is None) or df.empty:
            return

        df.to_csv(path_or_buf=self.filepath,
                  mode='a',
                  index=False,
                  header=not os.path.exists(self.filepath))

    def truncate(self, df: pd.DataFrame, **kwargs):
        os.remove(self.filepath)

    def recreate_storage(self, df: pd.DataFrame, **kwargs):
        df.to_csv(path_or_buf=self.filepath,
                  mode='w',
                  index=False,
                  header=True)

    def select_all(self, **kwargs) -> pd.DataFrame:
        data_type = {
            't': str,
            # 'mv': int,
            # 'vo': int
        }
        return pd.read_csv(self.filepath, dtype=data_type)


class KisvnTickStorageReader:
    def select_all(self) -> pd.DataFrame:
        raise NotImplementedError


class KisvnTickCSVFilesReader(KisvnTickStorageReader):
    def __init__(self, symbol, datadir, trade_dates):
        self.symbol = symbol
        self.datadir = datadir
        self.tick_date_storages = [KisvnRawTickCSVFile(symbol, datadir, td) for td in trade_dates]

    def select_all(self) -> pd.DataFrame:
        return pd.concat([storage.select_all() for storage in self.tick_date_storages])
