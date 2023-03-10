import pandas as pd
import requests
from pi_associates_library.binance.binance_depth_stream import DiffDepthStream
from pi_associates_library.binance.binance_url import BinanceHttpEndpoint


class BinanceOrderbook:
    def __init__(self, last_update_id, message_output_time, transaction_time, bids, asks, raw):
        self.last_update_id = last_update_id
        self.message_output_time = message_output_time
        self.transaction_time = transaction_time
        self.bids: pd.DataFrame = bids
        self.asks: pd.DataFrame = asks
        self.raw = raw


class BinanceOrderbookBuilder:
    @staticmethod
    def create_from_http_endpoint(symbol, session=None):
        url = BinanceHttpEndpoint.ORDERBOOK(symbol)
        if session:
            response = session.get(url)
        else:
            response = requests.get(url)
        response = response.json()
        return BinanceOrderbook(last_update_id=response['lastUpdateId'],
                                message_output_time=response['E'],
                                transaction_time=response['T'],
                                bids=pd.DataFrame(response['bids'], columns=['price', 'amount']),
                                asks=pd.DataFrame(response['asks'], columns=['price', 'amount']),
                                raw=response)

    @staticmethod
    def create_from_diff_depth_stream(orderbook: BinanceOrderbook, stream: DiffDepthStream):
        bids = orderbook.bids.copy()
        asks = orderbook.asks.copy()
        bids.update(stream.bids)
        asks.update(stream.asks)
        bids.drop(bids[bids.amount == 0], errors='ignore')
        asks.drop(asks[asks.amount == 0], errors='ignore')
        return BinanceOrderbook(last_update_id=stream.final_update_id,
                                message_output_time=stream.event_time,
                                transaction_time=stream.transaction_time,
                                bids=bids,
                                asks=asks,
                                raw=None)
