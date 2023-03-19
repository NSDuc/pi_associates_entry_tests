from datetime import datetime, date, time


class StockTradeTimeRange:
    def get_timeranges(self, trade_date: date):
        raise NotImplementedError


class HOSETradeTimeRange(StockTradeTimeRange):
    def get_timeranges(self, trade_date: date):
        # no trade on Saturday and Sunday
        if trade_date.weekday() in [5, 6]:
            return []

        begin1 = datetime.combine(trade_date, time(hour=9, minute=15, second=0, microsecond=0))
        end1 = datetime.combine(trade_date, time(hour=11, minute=30, second=0, microsecond=0))

        begin2 = datetime.combine(trade_date, time(hour=13, minute=00, second=0, microsecond=0))
        end2 = datetime.combine(trade_date, time(hour=14, minute=30, second=0, microsecond=0))

        begin3 = datetime.combine(trade_date, time(hour=14, minute=45, second=0, microsecond=0))
        end3 = datetime.combine(trade_date, time(hour=14, minute=45, second=0, microsecond=0))
        return [(begin1, end1), (begin2, end2), (begin3, end3)]
