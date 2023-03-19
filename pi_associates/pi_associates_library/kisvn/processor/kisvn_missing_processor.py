from pi_associates_library.dataframe_processor.dataframe_process import DataFrameProcessSequences
from pi_associates_library.dataframe_processor.dataframe_process_step import *
from pi_associates_library.kisvn.kisvn_tick_storage import KisvnRawTickCSVFile
from pi_associates_library.kisvn.processor.kisvn_processor import KisvnProcessor


class KisvnMissingProcessor(KisvnProcessor):
    def execute(self):
        for symbol in self.params.symbols():

            for td in self.params.process_dates():
                tick_storage = KisvnRawTickCSVFile(symbol,
                                                   datadir=self.params.processed_datadir(),
                                                   trade_date=td)
                try:
                    df = tick_storage.select_all()
                except FileNotFoundError:
                    self.logger.error(f'Datafile of {symbol} in date {td} not found !')
                    continue
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
                    # DataFrameDropColumns(['price_low',
                    #                       'price_high',
                    #                       'price_open',
                    #                       'price_close',
                    #                       'price_changed',
                    #                       'ra',
                    #                       'se',
                    #                       'stats_value'
                    #                       ]),
                    DataFrameApplyColumn(col='total_volume',
                                         apply_func=lambda row: row['stats_volume'] + row['trade_volume']),
                    DataFrameAddColByColShift(new_col=['prev_stats_volume', 'prev_trade_volume'],
                                              shift_col=['stats_volume', 'trade_volume'],
                                              shift=1, fill_value=None),
                    DataFrameAddColByColShift(new_col=['next_stats_volume', 'next_trade_volume'],
                                              shift_col=['stats_volume', 'trade_volume'],
                                              shift=-1, fill_value=None),
                    # KisvnTickPrint(),
                    DataFrameDropRowsIf('total_volume == next_stats_volume and '
                                        'stats_volume == prev_stats_volume+prev_trade_volume'),
                    # remove ATO if no miss next-ATO
                    DataFrameDropRowsIf("trade_type == 'UNKNOWN' and "
                                        "total_volume == next_stats_volume and "
                                        "stats_volume == 0"),
                    # remove ATC if no miss prev-ATC, or other trades after ATC
                    DataFrameDropRowsIf("next_stats_volume.isnull()"),

                    DataFrameApplyColumn(col='lost_volume',
                                         apply_func=lambda row: row['next_stats_volume'] - row['total_volume']),
                    DataFramePrintMissing('lost_volume'),
                ]

                for step in steps:
                    process.add_step(step)
                process.execute(df)
