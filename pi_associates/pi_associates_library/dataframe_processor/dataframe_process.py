from pi_associates_library.dataframe_processor.dataframe_process_step import DataFrameProcessStep, dataframe_rownum
from typing import List
import logging


class DataFrameProcessSequences:
    def __init__(self, symbol):
        self.logger = logging.getLogger(f'{self.__class__.__name__}.{symbol}')
        self.symbol = symbol
        self.steps: List[DataFrameProcessStep] = []

    def add_step(self, step: DataFrameProcessStep):
        self.steps.append(step)
        return self

    def create_steps(self, *steps):
        self.steps = [*steps]

    def execute(self, df):
        ori_df_rownum = dataframe_rownum(df)
        out_df = df

        self.logger.info(f"{'*'*25} [BEGIN] {'*'*25}")
        self.logger.info(f'[Begin] Input:{ori_df_rownum}')

        for i, step in enumerate(self.steps):
            self.logger.debug(f"[{i+1}/{len(self.steps)}] {step.name()} {step.desc()}")

            in_df = out_df
            out_df = step.execute(in_df)

            self.logger.info(f"[{i+1}/{len(self.steps)}] {step.name()} "
                             f"input:{dataframe_rownum(in_df)} row(s), output:{dataframe_rownum(out_df)} row(s), "
                             f"removed: {dataframe_rownum(in_df)-dataframe_rownum(out_df)}")

        self.logger.info(f"[End] input:{ori_df_rownum} row(s), output:{dataframe_rownum(out_df)} row(s), "
                         f"removed: {ori_df_rownum-dataframe_rownum(out_df)}\n\n\n\n")
        return out_df
