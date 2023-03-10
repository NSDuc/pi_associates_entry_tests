from pi_associates_library.binance.binance_url import BinanceWebSocket, BinanceHttpEndpoint
from pprint import pprint
from tenacity import retry, stop_after_attempt
import asyncio
import json
import pandas as pd
import requests
import websocket
import websockets


class BinanceTradeScraper:
    def __init__(self, symbol):
        self.symbol = symbol
        self.http_session = requests.session()

    @retry(stop=stop_after_attempt(1))
    def get_trades(self, limit=1000):
        response = self.http_session.get(BinanceHttpEndpoint.RECENT_TRADES_LIST(self.symbol, limit))
        return response.json()


class BinanceTradeAggregationDataBuilder:
    @staticmethod
    def reformat_data(json_data):
        for key in ['p', 'q']:
            json_data[key] = float(json_data[key])
        return pd.Series(json_data)

class BinanceTradeAggregationScraper:
    def __init__(self, symbol):
        self.symbol = symbol
        self.websocket = None
        self.async_websocket = None

    @retry(stop=stop_after_attempt(1))
    async def async_get_trade_aggregation(self):
        if self.async_websocket is None:
            self.async_websocket = await websockets.connect(BinanceWebSocket.AGGREGATE_TRADE_STREAMS(self.symbol))

        response = await asyncio.wait_for(self.websocket.recv(), timeout=2)
        trade_agg = json.loads(response)['data']
        return trade_agg

    @retry(stop=stop_after_attempt(1))
    def get_trade_aggregation(self):
        if self.websocket is None:
            self.websocket = websocket.create_connection(BinanceWebSocket.AGGREGATE_TRADE_STREAMS(self.symbol))

        response = self.websocket.recv()
        trade_agg = json.loads(response)['data']
        for k, v in trade_agg.items():
            print(f"KEY: {k, type(k)}")
            print(f"VAL: {v, type(v)}")

        return BinanceTradeAggregationDataBuilder.reformat_data(trade_agg)

class BinanceTradeAggregator:
    def __init__(self, symbol):
        self.symbol = symbol
        self.ws = None
        self.session = requests.session()

    async def connect_agg_trade_streams(self):
        return await websockets.connect(BinanceWebSocket.AGGREGATE_TRADE_STREAMS(self.symbol))

    @retry(stop=stop_after_attempt(1))
    async def live_aggregate(self, symbol):
        async with websockets.connect(BinanceWebSocket.AGGREGATE_TRADE_STREAMS(symbol)) as ws:
            response = requests.get(BinanceHttpEndpoint.RECENT_TRADES_LIST(symbol))
            df = pd.DataFrame(response.json())
            df_qty = df.groupby([df.qty]).size()
            print(df.tail(20))
            print(df.info())
            # print(df.sort_values(by=['qty']))
            # print(df_qty)
            count = 0
            while count < 3:

                response = await asyncio.wait_for(ws.recv(), timeout=2)
                # df = pandas.DataFrame(response.json())
                print('*'*50)
                trade_agg_ws = json.loads(response)['data']
                pprint(trade_agg_ws)
                count += 1

    @retry(stop=stop_after_attempt(1))
    async def async_get_trade_aggregator(self):
        if self.ws is None:
            self.ws = await self.connect_agg_trade_streams()

        response = await asyncio.wait_for(self.ws.recv(), timeout=2)
        trade_agg_ws = json.loads(response)['data']
        return trade_agg_ws

    @retry(stop=stop_after_attempt(1))
    def get_trade_aggregation(self):
        if self.ws is None:
            self.ws = websocket.create_connection(BinanceWebSocket.AGGREGATE_TRADE_STREAMS(self.symbol))

        response = self.ws.recv()
        trade_agg_ws = json.loads(response)['data']
        return trade_agg_ws

    @retry(stop=stop_after_attempt(1))
    def get_trades(self):
        response = self.session.get(BinanceHttpEndpoint.RECENT_TRADES_LIST(self.symbol))
        return response.json()

