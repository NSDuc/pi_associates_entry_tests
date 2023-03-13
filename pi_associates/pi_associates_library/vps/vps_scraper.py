from pi_associates_library.vps.vps_url import VPSWebSocketUrl
from tenacity import retry, stop_after_attempt
from typing import Optional
import json
import logging
import socketio


class VPSDataStorage:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_websocket_update_board_event(self, data):
        pass

    def on_websocket_update_stock_event(self, data):
        pass


class VPSWebSocketScraper:
    def __init__(self, symbol_list,
                 vps_websocket_data_handler: Optional[VPSDataStorage] = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.symbol_list = symbol_list
        self.vps_websocket_data_handler = vps_websocket_data_handler

    @retry(stop=stop_after_attempt(5))
    def init_websocket_connection(self):
        sio = socketio.Client(engineio_logger=True)

        @sio.event
        def connect():
            self.logger.info('[CONNECTION] connection established')
            regs_data = {
                "action": "join",
                "list": ",".join([sym for sym in self.symbol_list]),
            }
            regs_data = json.dumps(regs_data)
            sio.emit(event="regs", data=regs_data.encode('utf-8'))

        @sio.on("stock")
        def on_stock(data):
            self.logger.info(f"_____________[STOCK]: {data}")
            self.vps_websocket_data_handler.on_websocket_update_stock_event(data)

        @sio.on("board")
        def on_board(data):
            self.logger.info(f"_____________[BOARD] : {data}")
            self.vps_websocket_data_handler.on_websocket_update_board_event(data)

        @sio.event
        def disconnect():
            self.logger.info('[CONNECTION] disconnected from server')

        return sio

    @retry(stop=stop_after_attempt(3))
    def run_forever(self):
        sio = self.init_websocket_connection()
        sio.connect(VPSWebSocketUrl.BG_DATAFEED(), transports=['websocket'])
        sio.wait()
        # time.sleep(3)
