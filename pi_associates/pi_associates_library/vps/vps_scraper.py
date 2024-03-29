from pi_associates_library.vps.vps_url import VPSWebSocketUrl
from tenacity import retry, stop_after_attempt, wait_fixed
from typing import Optional
import json
import logging
import socketio
import threading


class VPSWebSocketScraperHandler:
    def __init__(self, symbol_list):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.symbol_list = symbol_list
        self._locks = {sym: threading.Lock() for sym in symbol_list}

    def on_websocket_update_board_event(self, data):
        if 'sym' not in data:
            return
        sym = data['sym']

        with self._locks.get(sym):
            self.logger.info(f"[message 'board' for {sym}] {data}")

    def on_websocket_update_stock_event(self, data):
        if 'sym' not in data:
            return
        sym = data['sym']

        with self._locks.get(sym):
            self.logger.info(f"[message 'event' for {sym}] {data}")


class VPSWebSocketScraper:
    def __init__(self, symbol_list,
                 vps_websocket_handler: Optional[VPSWebSocketScraperHandler] = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.symbol_list = symbol_list
        self.vps_websocket_data_handler = vps_websocket_handler

    @retry(stop=stop_after_attempt(5))
    def init_websocket_connection(self):
        sio = socketio.Client(engineio_logger=False, logger=False)
        logging.getLogger('engineio.client').setLevel(logging.WARN)
        logging.getLogger('socketio.client').setLevel(logging.WARN)

        @sio.event
        def connect():
            self.logger.info('Connection established')
            regs_data = {
                "action": "join",
                "list": ",".join([sym for sym in self.symbol_list]),
            }
            regs_data = json.dumps(regs_data)
            sio.emit(event="regs", data=regs_data.encode('utf-8'))

        @sio.on("stock")
        def on_stock(data):
            self.vps_websocket_data_handler.on_websocket_update_stock_event(data['data'])

        @sio.on("board")
        def on_board(data):
            self.vps_websocket_data_handler.on_websocket_update_board_event(data['data'])

        @sio.event
        def disconnect():
            self.logger.info('Disconnected from server')

        return sio

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def run_forever(self):
        sio = self.init_websocket_connection()
        sio.connect(VPSWebSocketUrl.BG_DATAFEED(), transports=['websocket'])
        sio.wait()
