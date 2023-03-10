from pi_associates_library.config_loader import EnvironConfigLoader
from pi_associates_library.job_runner import ProcessJobRunner, ThreadJobRunner, JobRunner
from pi_associates_library.vps.vps_url import VPSWebSocketUrl
from datetime import datetime
from pprint import pprint
from tenacity import retry, stop_after_attempt
import json
import logging
import os.path
import requests
import socketio
import time



class VPSWebSocketScraper:
    def __init__(self, symbol_list):
        self.symbol_list = symbol_list
        self.logger = logging.getLogger(self.__class__.__name__)

        self.__sio = None

    @retry(stop=stop_after_attempt(5))
    def init_websocket_connection(self):
        sio = socketio.Client(engineio_logger=True)

        @sio.event
        def connect():
            self.logger.info('[CONNECTION] connection established')
            regs_data = {
                "action" : "join",
                "list" : ",".join([sym for sym in self.symbol_list if sym != 'VN30']),
            }
            regs_data = json.dumps(regs_data)
            sio.emit(event="regs", data=regs_data.encode('utf-8'))


        # @sio.event
        # def message(data):
        #     self.logger.info(f'[MESSAGE] {data}')
        #     # sio.emit('my response', {'response': 'my response'})


        @sio.on("stock")
        def on_stock(data):
            self.logger.info("_____________[STOCK]")
            self.logger.info(type(data))
            self.logger.info(len(data))
            self.logger.info(data)

        @sio.on("board")
        def on_board(data):
            self.logger.info("_____________[BOARD]")
            self.logger.info(type(data))
            self.logger.info(len(data))
            self.logger.info(data)


        @sio.event
        def disconnect():
            self.logger.info('[CONNECTION] disconnected from server')

        return sio


    @retry(stop=stop_after_attempt(3))
    def run_forever(self):
        sio = self.init_websocket_connection()
        sio.connect(VPSWebSocketUrl.BG_DATAFEED(), transports=['websocket'])
        # sio.wait()
        time.sleep(3)


if __name__ == '__main__':
    job_runner: JobRunner = ThreadJobRunner()
    config: dict = EnvironConfigLoader().load_config()

    base_dirpath = config.get('PI_ASSOCIATES_VPS_RAW_DATA_DIRPATH')
    log_dirpath = config.get('PI_ASSOCIATES_VPS_LOG_DIRPATH')
    symbol_list = config.get('PI_ASSOCIATES_VNINDEX_SYMBOLS').split(',')

    now = datetime.now().strftime("%H%M%S")
    logging.basicConfig(filename=os.path.join(log_dirpath, f'Entry2_Answer1_{now}.log'), level=logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler())

    
    VPSWebSocketScraper(symbol_list).run_forever()