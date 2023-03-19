from pi_associates_library.dataframe_processor.dataframe_process import DataFrameProcessSequences
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnRawTickCSVFile
from pi_associates_library.kisvn.processor.kisvn_processor import KisvnProcessor
from pi_associates_library.dataframe_processor.dataframe_process_step import *
from datetime import datetime, timedelta


class KisvnRawProcessor(KisvnProcessor):
    def execute(self):
        for d in self.params.process_dates():
            for symbol in self.params.symbols():
                self.logger.info(f'Process for {symbol} in {d.isoformat()}')
                raw_tick_storage = KisvnRawTickCSVFile(symbol,
                                                       datadir=self.params.raw_datadir(),
                                                       trade_date=d)
                processed_tick_storage = KisvnRawTickCSVFile(symbol,
                                                             datadir=self.params.processed_datadir(),
                                                             trade_date=d)
                try:
                    df = raw_tick_storage.select_all()
                except FileNotFoundError:
                    self.logger.error(f'Datafile of {symbol} in date {d} not found !')
                    return

                process_seq = DataFrameProcessSequences(symbol=symbol)
                process_steps = [
                    DataFrameRenameColumns(rename={
                        'ch': 'price_changed',
                        'o': 'price_open',
                        'c': 'price_close',
                        'h': 'price_high',
                        'l': 'price_low',
                        't': 'trade_time',
                        'mb': 'trade_type',
                        'mv': 'trade_volume',
                        'ra': 'ra',
                        'se': 'se',
                        'va': 'stats_value',
                        'vo': 'stats_volume',
                    }),
                    DataFrameDropNotANumRows(),
                    DataFrameSortRowsByCols(by=['stats_volume'], ascending=True),
                    DataFrameDropDuplicatedRows(keep='first'),
                    DataFrameDropRowsIf('trade_volume == 0'),
                    DataFrameDropDuplicatedRowsBy(cols=['stats_volume'], query='stats_value == 0'),
                    DataFrameApplyColumn(
                        col='trade_time',
                        apply_func=lambda row: datetime.combine(d, datetime.strptime(row['trade_time'], "%H%M%S").time()) + timedelta(hours=7)
                    ),
                    DataFrameAssertColIncrease(col='stats_volume'),
                    DataFrameAssertColIncrease(col='trade_time'),
                    # DataFrameAssertRow(
                    #     row_id=0,
                    #     assert_func=lambda r: (r['trade_type'] == 'UNKNOWN' and (r['stats_volume'] == 0)) or (r['trade_type'] != 'UNKNOWN')
                    # ),
                ]

                process_seq.create_steps(*process_steps)

                df = process_seq.execute(df)

                processed_tick_storage.recreate_storage(df)
