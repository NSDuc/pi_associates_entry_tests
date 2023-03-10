from datetime import datetime
import os.path
import pandas as pd


class KisvnTickStorage:
    def __init__(self, stock):
        self.stock = stock

    def insert(self, df: pd.DataFrame, **kwargs):
        raise NotImplementedError

    def select_all(self, **kwargs) -> pd.DataFrame:
        raise NotImplementedError


class KisvnTickCSVFile(KisvnTickStorage):
    def __init__(self, stock, base_dirpath):
        super().__init__(stock)
        self.base_dirpath = base_dirpath

    def get_dir_and_file_path(self, date: datetime):
        date_dir_name = date.strftime("%d%m%Y")
        date_dir_path = os.path.join(self.base_dirpath, date_dir_name)
        filepath = os.path.join(date_dir_path, f"{self.stock}.csv")
        return date_dir_path, filepath

    def insert(self, df: pd.DataFrame, **kwargs):
        dirpath, filepath = self.get_dir_and_file_path(kwargs['date'])
        os.makedirs(dirpath, exist_ok=True)
        df.to_csv(path_or_buf=filepath,
                  mode='a',
                  index=False,
                  header=not os.path.exists(filepath))

    def select_all(self, **kwargs):
        date_dirpath, filepath = self.get_dir_and_file_path(kwargs['date'])
        data_type = {
            't' : str,
            'mv': int,
            'vo': int
        }
        return pd.read_csv(filepath, dtype=data_type)


class KisvnProcessedTickPersistent:
    def __init__(self, stock):
        self.stock = stock


class KisvnProcessedTickCSVFile(KisvnProcessedTickPersistent):
    def __init__(self, stock, data_dirpath):
        super().__init__(stock)
        self.data_dirpath = data_dirpath

    def insert(self, df: pd.DataFrame, **kwargs):

        pass


