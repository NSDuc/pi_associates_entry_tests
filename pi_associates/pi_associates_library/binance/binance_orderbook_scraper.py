import requests

from pi_associates_library.binance.binance_depth_stream import DiffDepthStream
from pi_associates_library.binance.binance_orderbook import BinanceOrderbook, BinanceOrderbookBuilder
from pi_associates_library.binance.binance_orderbook_storage import BinanceOrderbookStorage
from pi_associates_library.binance.binance_url import BinanceWebSocket
from tenacity import retry, stop_after_attempt
from typing import Optional
import enum
import asyncio
import websockets


class DiffDepthStreamCompatible(enum.Enum):
    OLD_STREAM_THEN_SKIP = enum.auto()
    LOST_STREAM_THEN_RESET = enum.auto()
    NEXT_STREAM_THEN_UPDATE = enum.auto()

    @staticmethod
    def check_compatible(orderbook: BinanceOrderbook,
                         stream: DiffDepthStream, last_stream: Optional[DiffDepthStream]):
        if stream.final_update_id < orderbook.last_update_id:
            return DiffDepthStreamCompatible.OLD_STREAM_THEN_SKIP

        if last_stream:
            if last_stream.final_update_id == stream.final_update_id_in_last_stream:
                return DiffDepthStreamCompatible.NEXT_STREAM_THEN_UPDATE
        else:
            if stream.first_update_id <= orderbook.last_update_id <= stream.final_update_id:
                return DiffDepthStreamCompatible.NEXT_STREAM_THEN_UPDATE

        return DiffDepthStreamCompatible.LOST_STREAM_THEN_RESET


class BinanceOrderbookScraper:
    def __init__(self, symbol, orderbook_storage: BinanceOrderbookStorage):
        self.symbol = symbol
        self.orderbook_storage = orderbook_storage
        self.http_session = None
        # self.http_session = requests.session()

    def get_orderbook_from_http_endpoint(self):
        return BinanceOrderbookBuilder.create_from_http_endpoint(self.symbol, self.http_session)

    @retry(stop=stop_after_attempt(1))
    async def scrape(self):
        async with websockets.connect(BinanceWebSocket.DIFF_BOOK_DEPTH_STREAM(self.symbol)) as ws:
            orderbook = None
            last_stream = None
            while True:
                if orderbook is None:
                    # orderbook = BinanceOrderbookBuilder.create_from_http_endpoint(self.symbol)
                    orderbook = self.get_orderbook_from_http_endpoint()

                if orderbook:
                    self.orderbook_storage.insert_orderbook(self.symbol, orderbook)

                response = await asyncio.wait_for(ws.recv(), timeout=2)
                stream = DiffDepthStream.create_from_websocket_stream(response)

                compatible = DiffDepthStreamCompatible.check_compatible(orderbook, stream, last_stream)
                print(f"compatible = {compatible}")
                if compatible == DiffDepthStreamCompatible.OLD_STREAM_THEN_SKIP:
                    continue
                elif compatible == DiffDepthStreamCompatible.LOST_STREAM_THEN_RESET:
                    orderbook = last_stream = None
                elif compatible == DiffDepthStreamCompatible.NEXT_STREAM_THEN_UPDATE:
                    orderbook = BinanceOrderbookBuilder.create_from_diff_depth_stream(orderbook, stream)
                    last_stream = stream


if __name__ == '__main__':
    symbol = 'btcusdt'
    orderbook_storage = BinanceOrderbookStorage(symbol)
    orderbook_scraper = BinanceOrderbookScraper(symbol, orderbook_storage)
    asyncio.get_event_loop().run_until_complete(orderbook_scraper.scrape())
