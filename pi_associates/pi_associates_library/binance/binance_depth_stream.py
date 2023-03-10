import json
import pandas as pd


class DiffDepthStream:
    @staticmethod
    def create_from_websocket_stream(response):
        response = json.loads(response)
        data = response['data']
        return DiffDepthStream(event_type=data['e'],
                               event_time=data['E'],
                               transaction_time=data['T'],
                               first_update_id_in_event=data['U'],
                               final_update_id_in_event=data['u'],
                               final_update_id_in_last_stream=data['pu'],
                               bids=pd.DataFrame(data['b']),
                               asks=pd.DataFrame(data['a']))

    def __init__(self, event_type, event_time, transaction_time,
                 first_update_id_in_event, final_update_id_in_event, final_update_id_in_last_stream,
                 bids, asks):
        self.event_type = event_type
        self.event_time = event_time
        self.transaction_time = transaction_time
        self.first_update_id = first_update_id_in_event
        self.final_update_id = final_update_id_in_event
        self.final_update_id_in_last_stream = final_update_id_in_last_stream
        self.bids = bids
        self.asks = asks
