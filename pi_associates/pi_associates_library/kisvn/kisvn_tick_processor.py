from datetime import datetime
import pandas as pd


class KisvnTickProcessor:
    @staticmethod
    def clean_raw_tickdata(df: pd.DataFrame):
        # remove duplicates data
        df1: pd.DataFrame = df.drop_duplicates(subset=None, keep='first',
                                               inplace=False, ignore_index=True)

        # sort by trade-volume
        df2 = df1.sort_values(by=['vo'], ascending=True,
                              inplace=False, ignore_index=False)

        df2['t'] = df2.apply(lambda row: datetime.strptime(row.t, "%H%M%S").time(), axis=1)

        df2.reset_index(drop=True)

        return df2

    @staticmethod
    def get_missing_tickdata(df: pd.DataFrame):
        df['result_vo'] = df.apply(lambda row: row.vo + row.mv, axis=1)
        # df1['t'] = df1.apply(lambda row: datetime.strptime(row.t, "%H%M%S").time(), axis=1)


        # df2 = pd.merge(df, df,
        #                how='outer',
        #                left_on='final_vo', right_on='vo')

        # print(f"\n{'*'*20}\ndf3")
        # print(df3)
        # exit(0)
        return df
