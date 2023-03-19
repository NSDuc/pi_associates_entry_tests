
class BinanceHttpUrl:
    @staticmethod
    def ORDERBOOK(symbol):
        return f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"

    @staticmethod
    def RECENT_TRADES_LIST(symbol, limit=1000):
        return f"https://fapi.binance.com/fapi/v1/trades?symbol={symbol}&limit={limit}"


class BinanceWebSocketUrl:
    # Aggregate Trade Streams
    # Only market trades will be aggregated, which means the insurance fund trades and ADL trades won't be aggregated.
    @staticmethod
    def AGGREGATE_TRADE_STREAMS(symbol):
        return f"wss://fstream.binance.com/stream?streams={symbol}@aggTrade"

    # Diff. Book Depth Streams
    @staticmethod
    def DIFF_BOOK_DEPTH_STREAM(symbol):
        return f"wss://fstream.binance.com/stream?streams={symbol}@depth"

