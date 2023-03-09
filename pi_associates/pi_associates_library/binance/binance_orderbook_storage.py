from pi_associates_library.binance.binance_orderbook import BinanceOrderbook


class BinanceOrderbookStorage:
    def __init__(self, symbol):
        self.symbol = symbol

    def insert_orderbook(self, symbol, orderbook: BinanceOrderbook):
        print(f"\n\n{'*'*50}")
        print(f"last_update_id = {orderbook.last_update_id}")
        print(orderbook.bids)
        print(orderbook.asks)
        return


class KafkaBinanceOrderbookStorage(BinanceOrderbookStorage):
    def __init__(self, symbol):
        super().__init__(symbol)

    def insert_orderbook(self, symbol, orderbook: BinanceOrderbook):
        super().insert_orderbook(symbol, orderbook)
