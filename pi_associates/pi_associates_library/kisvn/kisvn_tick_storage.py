from datetime import datetime
import os.path
import pandas as pd


class KisvnTickStorage:
    def __init__(self, stock):
        self.stock = stock

    def add_tickdata(self, df: pd.DataFrame, trade_date):
        raise NotImplementedError

    def overwrite_tickdata(self, df: pd.DataFrame, trade_date):
        raise NotImplementedError

    def read_tickdata(self, trade_date) -> pd.DataFrame:
        raise NotImplementedError


class KisvnTickCSVFile(KisvnTickStorage):
    def __init__(self, stock, data_dirpath):
        super().__init__(stock)
        self.data_dirpath = data_dirpath

    def __get_tick_file_path(self, trade_date: datetime):
        partition_dirname = trade_date.strftime("%d%m%Y")
        partition_dirpath = os.path.join(self.data_dirpath, partition_dirname)
        os.makedirs(partition_dirpath, exist_ok=True)

        filepath = os.path.join(partition_dirpath, f"{self.stock}.csv")
        return filepath

    def add_tickdata(self, df: pd.DataFrame, trade_date):
        filepath = self.__get_tick_file_path(trade_date)
        df.to_csv(path_or_buf=filepath,
                  mode='a',
                  index=False,
                  header=not os.path.exists(filepath))

    def overwrite_tickdata(self, df: pd.DataFrame, trade_date):
        filepath = self.__get_tick_file_path(trade_date)
        df.to_csv(path_or_buf=filepath,
                  mode='w',
                  index=False,
                  header=True)

    def read_tickdata(self, trade_date):
        filepath = self.__get_tick_file_path(trade_date)
        data_type = {
            't' : str,
            'mv': int,
            'vo': int
        }
        return pd.read_csv(filepath, dtype=data_type)
