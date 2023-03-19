from pi_associates_library.dataframe_processor.dataframe_process_step import dataframe_rownum
from typing import Optional
import pandas as pd


class KisvnScraperDelayStrategy:
    def calculate_delay(self, df: Optional[pd.DataFrame]) -> float:
        raise NotImplementedError

    def maximum_delay_in_seconds(self):
        raise NotImplementedError


class KisvnScraperFixedDelay(KisvnScraperDelayStrategy):
    def calculate_delay(self, df: Optional[pd.DataFrame]) -> float:
        return 10.0

    def maximum_delay_in_seconds(self):
        return 10.0


class KisvnScraperDynamicDelay(KisvnScraperDelayStrategy):
    def __init__(self, min_delay=5.0, max_delay=20.0, immediate_delay=0.1, immediate_dataframe_size=80):
        self.max_delay = max_delay
        self.min_delay = min_delay
        self.immediate_delay = immediate_delay
        self.immediate_dataframe_size = immediate_dataframe_size
        self.delta = self.max_delay - self.min_delay

    def calculate_delay(self, df: Optional[pd.DataFrame]) -> float:
        if (df is None) or df.empty:
            return self.min_delay

        if dataframe_rownum(df) >= self.immediate_dataframe_size:
            return self.immediate_delay

        percent = dataframe_rownum(df) / self.immediate_dataframe_size
        return max(self.max_delay - self.delta * percent, self.min_delay)

    def maximum_delay_in_seconds(self):
        return self.max_delay
