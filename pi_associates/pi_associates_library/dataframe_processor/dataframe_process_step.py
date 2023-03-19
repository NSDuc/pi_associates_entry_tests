from typing import Literal
import inspect
import json
import logging
import re
import pandas as pd


def dataframe_rownum(df: pd.DataFrame):
    return len(df.index)


class DataFrameProcessStep:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def name(self):
        name = re.findall('[A-Z][^A-Z]*', self.__class__.__name__)
        if name[0] == 'Data' and name[1] == 'Frame':
            name = name[2:]
        return '-'.join(name)

    def desc(self):
        return ''


class DataFramePrint(DataFrameProcessStep):
    def __init__(self, info=False, dataframe=True):
        super().__init__()
        self.print_info = info
        self.print_dataframe = dataframe

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.print_info:
            self.logger.info(f"info:\n{df.info()}")
        if self.print_dataframe:
            self.logger.info(f"data:\n{df}")
        return df


class DataFrameDropNotANumRows(DataFrameProcessStep):
    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        nan_df = df[df.isnull().any(axis=1)]
        if dataframe_rownum(nan_df):
            self.logger.debug(f'{dataframe_rownum(nan_df)} row(s) contain NaN value')
            self.logger.debug(f'NaN rows: \n{nan_df}')
        return df.drop(nan_df.index)


class DataFrameDropDuplicatedRows(DataFrameProcessStep):
    def __init__(self, cols=None, keep: Literal['first', 'last', False] = 'first'):
        super().__init__()
        self.cols = cols
        self.keep = keep

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        dup_df = df[df.duplicated(subset=self.cols, keep=False)]
        if dataframe_rownum(dup_df):
            self.logger.debug(f'{dataframe_rownum(dup_df)} duplicated rows')
            self.logger.debug(f'duplicated row pairs: \n{dup_df}')
        return df.drop_duplicates(subset=self.cols, keep=self.keep,
                                  inplace=False, ignore_index=True)

    def desc(self):
        return f"in {','.join(self.cols) if self.cols else 'ALL columns'}, keep {self.keep}"


class DataFrameDropDuplicatedRowsBy(DataFrameProcessStep):
    def __init__(self, cols, query):
        super().__init__()
        self.cols = cols
        self.query = query

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        dup_df = df[df.duplicated(subset=self.cols, keep=False)]
        if dataframe_rownum(dup_df):
            self.logger.info(f'{dataframe_rownum(dup_df)/2} duplicated row pairs')
            self.logger.debug(f'Duplicated row pairs: \n{dup_df}')

        invalid_df = dup_df.query(self.query)
        self.logger.debug(f'invalid rows is:\n{invalid_df}\n')
        return df.drop(invalid_df.index)

    def desc(self):
        return f"{self.cols} if {self.query}"


class DataFrameDropIncompleteRows(DataFrameProcessStep):
    def __init__(self, cols=None):
        super().__init__()
        self.cols = cols

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df0 = df.dropna(axis=1)
        self.logger.debug(f'Dropped rows: \n{df0}')
        return df0

    def desc(self):
        return f'drop row with empty value'


class DataFrameApplyColumn(DataFrameProcessStep):
    def __init__(self, col, apply_func):
        super().__init__()
        self.col = col
        self.apply_func = apply_func

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.col] = df.apply(self.apply_func, axis=1)
        return df

    def desc(self):
        return f'[{self.col}] operation: {inspect.getsource(self.apply_func).lstrip()}'


class DataFrameDropRowsIf(DataFrameProcessStep):
    def __init__(self, query):
        super().__init__()
        self.query = query

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        drop_df = df.query(self.query)
        df = df.drop(drop_df.index)
        if dataframe_rownum(drop_df):
            self.logger.debug(f'Dropped {dataframe_rownum(drop_df)} rows')
            self.logger.debug(f'Dropped rows: \n{drop_df}')
        return df

    def desc(self):
        return self.query


class DataFrameSortRowsByCols(DataFrameProcessStep):
    def __init__(self, by, ascending=True):
        super().__init__()
        self.by = by
        self.ascending = ascending

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(by=self.by, ascending=self.ascending,
                            inplace=False, ignore_index=False)
        df = df.reset_index(drop=True)
        return df

    def desc(self):
        return f'{self.by}, ascending={self.ascending}'


class DataFrameAssertMonotonicIncr(DataFrameProcessStep):
    def __init__(self, cols):
        super().__init__()
        self.cols = cols

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.cols:
            if df[col].is_monotonic_increasing:
                self.logger.info(f"Column <{col}> is monotonic increasing")
            else:
                self.logger.error(f"Column <{col}> is NOT monotonic increasing")
                raise AssertionError
        return df

    def desc(self):
        return self.cols


class DataFrameAssertNotMonoDesc(DataFrameProcessStep):
    def __init__(self, cols):
        super().__init__()
        self.cols = cols

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.cols:
            if not df[col].is_monotonic_decreasing:
                self.logger.info(f"Column <{col}> is monotonic increasing")
            else:
                self.logger.error(f"Column <{col}> is monotonic increasing")
                raise AssertionError
        return df

    def desc(self):
        return self.cols


class DataFrameAssertColIncrease(DataFrameProcessStep):
    def __init__(self, col):
        super().__init__()
        self.col = col
        self.next_col = 'next_' + col
        self.prev_col = 'prev_' + col

        self.query = f'{self.prev_col}.isnull() |' \
                     f'(({self.prev_col} <= {self.col}) & ({self.col} <= {self.next_col})) | ' \
                     f'{self.next_col}.isnull()'

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df0 = df.copy(deep=True)
        df0[self.prev_col] = df0[self.col].shift(1, fill_value=None)
        df0[self.next_col] = df0[self.col].shift(-1, fill_value=None)
        df1 = df0.query(self.query)
        df0 = df0.drop(df1.index)
        if dataframe_rownum(df0):
            self.logger.error(f'Rows are not mono-increase [{self.col}]:\n{df0} ')
            raise AssertionError
        return df

    def desc(self):
        return self.query


class DataFrameAssertRow(DataFrameProcessStep):
    def __init__(self, row_id, assert_func):
        super().__init__()
        self.row_id = row_id
        self.assert_func = assert_func

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        data = df.iloc[self.row_id].to_dict()
        if self.assert_func(data):
            return df
        raise AssertionError

    def desc(self):
        return f"row_id={self.row_id} {inspect.getsource(self.assert_func).lstrip()}"


class DataFrameRenameColumns(DataFrameProcessStep):
    def __init__(self, rename):
        super().__init__()
        self.rename = rename

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(self.rename, axis=1)
        return df

    def desc(self):
        return f'\n{json.dumps(self.rename, indent=4)}'


class DataFrameDropColumns(DataFrameProcessStep):
    def __init__(self, drop_cols):
        super().__init__()
        self.drop_cols = drop_cols

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(self.drop_cols, axis=1)
        return df

    def desc(self):
        return self.drop_cols


class DataFrameAddColByColShift(DataFrameProcessStep):
    def __init__(self, new_col, shift_col, shift, fill_value=None):
        super().__init__()
        self.new_col = new_col
        self.shift_col = shift_col
        self.shift = shift
        self.fill_value = fill_value

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        df0 = df.copy()
        df0[self.new_col] = df0[self.shift_col].shift(self.shift, fill_value=self.fill_value)
        return df0


class DataFrameGroupAgg(DataFrameProcessStep):
    def __init__(self, group_col, agg):
        super().__init__()
        self.group_col = group_col
        self.agg = agg

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        group_df = df.groupby(self.group_col)
        return group_df.agg(self.agg)


class DataFramePrintNumericSumPerCol(DataFrameProcessStep):
    def __init__(self, col=None, sum_desc='total per column'):
        super().__init__()
        self.col = col
        self.sum_desc = sum_desc

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        def human_readable_num(num):
            if num < 1000:
                return str(num)
            elif num < 1000000:
                return str(num/1000) + 'K'
            else:
                return str(num/1000000) + 'M'

        display_df = df.copy()
        display_df.loc[self.sum_desc] = display_df.sum(numeric_only=True, axis=0)

        for col in display_df.columns:
            display_df.at[self.sum_desc, col] = human_readable_num(display_df.at[self.sum_desc, col])

        self.logger.info(f'SUM per column:\n{display_df}')

        return df


class DataFramePrintMissing(DataFrameProcessStep):
    def __init__(self, col):
        super().__init__()
        self.col = col

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"Missing tick data PAIRS:\n{df}")
        self.logger.info(f'SUM of [{self.col}] is {df[self.col].sum()}')
        return df
