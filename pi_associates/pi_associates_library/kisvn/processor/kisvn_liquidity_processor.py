from pi_associates_library.dataframe_processor.dataframe_process import DataFrameProcessSequences
from pi_associates_library.dataframe_processor.dataframe_process_step import *
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnRawTickCSVFile
from pi_associates_library.kisvn.processor.kisvn_processor import KisvnProcessor, KisvnProcessorParams
from datetime import datetime
import pandas as pd


class KisvnLiquidProcessor(KisvnProcessor):
    def __init__(self, params: KisvnProcessorParams):
        super().__init__(params)
        self.symbol_to_dataframe = {}

    def execute(self):
        # Map phase
        for symbol in self.params.symbols():
            df_list = []
            for d in sorted(self.params.process_dates()):
                tick_storage = KisvnRawTickCSVFile(symbol,
                                                   datadir=self.params.processed_datadir(),
                                                   trade_date=d)
                try:
                    df = tick_storage.select_all()
                    df_list.append(df)
                except FileNotFoundError:
                    self.logger.error(f'Datafile of {symbol} in date {d} not found !')
                    return

            self.symbol_to_dataframe[symbol] = pd.concat([*df_list], ignore_index=True)
            self.symbol_to_dataframe[symbol].reset_index(drop=True, inplace=True)

            '''
                price_changed
                price_open
                price_close
                price_high
                price_low
                trade_time
                trade_type
                trade_volume
                ra
                se
                stats_value
                stats_volume
            '''

            process = DataFrameProcessSequences(symbol)
            steps = [
                DataFrameApplyColumn(
                    col='trade_time',
                    apply_func=lambda r: datetime.strptime(r['trade_time'], '%Y-%m-%d %H:%M:%S')
                ),
                DataFrameApplyColumn(
                    col='trade_hour',
                    apply_func=lambda r: r['trade_time'].replace(minute=0, second=0, microsecond=0)
                ),
                DataFrameApplyColumn(
                    col='total_volume',
                    apply_func=lambda r: r['trade_volume'] + r['stats_volume']
                ),
                DataFrameGroupAgg(
                    group_col='trade_hour',
                    agg={
                        'total_volume': 'max',
                        'stats_volume': 'min',
                    }
                ),
                DataFrameApplyColumn(
                    col=f'{symbol}',
                    apply_func=lambda r: r['total_volume'] - r['stats_volume']
                ),
                DataFrameDropColumns(['total_volume', 'stats_volume']),
                DataFramePrint(),
            ]

            for step in steps:
                process.add_step(step)
            self.symbol_to_dataframe[symbol] = process.execute(self.symbol_to_dataframe[symbol])

        # Reduce phase
        vn30_df = pd.concat(self.symbol_to_dataframe.values(), axis='columns')
        process = DataFrameProcessSequences('VN30')
        process.create_steps(
            DataFrameApplyColumn(col='VN30',
                                 apply_func=lambda r: sum(r[s] for s in self.params.symbols())),
            DataFramePrintNumericSumPerCol(sum_desc='Total-Trade-Volume'),
        )
        process.execute(vn30_df)


class KisvnEstimateLiquidProcessor(KisvnProcessor):
    def __init__(self, params: KisvnProcessorParams):
        super().__init__(params)
        self.symbol_to_dataframe = {}

    def execute(self):
        # Map phase
        for symbol in self.params.symbols():
            df_list = []
            for d in sorted(self.params.process_dates()):
                tick_storage = KisvnRawTickCSVFile(symbol,
                                                   datadir=self.params.processed_datadir(),
                                                   trade_date=d)
                try:
                    vn30_df = tick_storage.select_all()
                    df_list.append(vn30_df)
                except FileNotFoundError:
                    self.logger.error(f'Datafile of {symbol} in date {d} not found !')
                    return

            self.symbol_to_dataframe[symbol] = pd.concat([*df_list], ignore_index=True)
            self.symbol_to_dataframe[symbol].reset_index(drop=True, inplace=True)

            process = DataFrameProcessSequences(symbol)
            steps = [
                DataFrameApplyColumn(
                    col='trade_time',
                    apply_func=lambda r: datetime.strptime(r['trade_time'], '%Y-%m-%d %H:%M:%S')
                ),
                DataFrameApplyColumn(
                    col='trade_hour',
                    apply_func=lambda r: r['trade_time'].replace(minute=0, second=0, microsecond=0)
                ),
                DataFrameAddColByColShift(new_col=['next_trade_time', 'next_stats_volume'],
                                          shift_col=['trade_time', 'stats_volume'],
                                          shift=-1, fill_value=None),
                DataFrameApplyColumn(
                    col='next_is_same_day',
                    apply_func=lambda r: r['next_trade_time'] and r['trade_time'].day == r['next_trade_time'].day
                ),
                DataFrameApplyColumn(
                    col=f'estimate_trade_volume',
                    apply_func=lambda r: max(r['next_stats_volume'] - r['stats_volume'],
                                             r['trade_volume']) if r['next_is_same_day'] else r['trade_volume']
                ),
                DataFrameGroupAgg(
                    group_col='trade_hour',
                    agg={
                        'estimate_trade_volume': 'sum',
                    }
                ),
                DataFrameApplyColumn(
                    col=symbol,
                    apply_func=lambda r: r['estimate_trade_volume']
                ),
                DataFrameDropColumns(['estimate_trade_volume']),
                DataFramePrint(),
            ]

            for step in steps:
                process.add_step(step)
            self.symbol_to_dataframe[symbol] = process.execute(self.symbol_to_dataframe[symbol])

        # Reduce phase
        vn30_df = pd.concat(self.symbol_to_dataframe.values(), axis='columns')
        process = DataFrameProcessSequences('VN30')
        process.create_steps(
            DataFrameApplyColumn(col='VN30',
                                 apply_func=lambda r: sum(r[s] for s in self.params.symbols())),
            DataFramePrintNumericSumPerCol(sum_desc='Total-Trade-Volume'),
        )
        process.execute(vn30_df)
