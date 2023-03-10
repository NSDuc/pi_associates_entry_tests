import socketio
import requests

from pi_associates_library.vps.vps_url import VPSWebSocketUrl

session = requests.session()
# sio = socketio.Client(engineio_logger=True, http_session=session)
# sio = socketio.Client(engineio_logger=True)
sio = socketio.Client()

headers = {
    "Connection": "keep-alive",
    "Accept": "*/*",
    # "Host": "bgdatafeed.vps.com.vn",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:104.0) Gecko/20100101 Firefox/104.0",
    # "Accept-Encoding": "gzip, deflate, br",
    # "Content-type": "text/plain;charset=UTF-8",
    # "Content-Length": "171",
    "Origin": "https://banggia.vps.com.vn",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://banggia.vps.com.vn/",
    "Accept-Language": "en-US,en;q=0.5",
    # "Cookie": "_ga_790K9595DC=GS1.1.1678512501.3.1.1678514153.0.0.0; _ga=GA1.1.1199258199.1678111945; _ga_QW53DJZL1X=GS1.1.1678111966.1.1.1678113655.0.0.0; io=Ko--hRj53n0FG4qXAebD",
}

ws_headers = {
    "Host" : " bgdatafeed.vps.com.vn",
    "User-Agent" : " Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:104.0) Gecko/20100101 Firefox/104.0",
    "Accept" : " */*",
    "Accept-Language" : " en-US,en;q=0.5",
    "Accept-Encoding" : " gzip, deflate, br",
    "Sec-WebSocket-Version" : " 13",
    "Origin" : " https://banggia.vps.com.vn",
    "Sec-WebSocket-Extensions" : " permessage-deflate",
    "Sec-WebSocket-Key" : " JkfQpSLnbp5D1GuMGS/N2A==",
    "Connection" : " keep-alive, Upgrade",
    # "Cookie" : " _ga_790K9595DC=GS1.1.1678512501.3.1.1678514954.0.0.0; _ga=GA1.1.1199258199.1678111945; _ga_QW53DJZL1X=GS1.1.1678111966.1.1.1678113655.0.0.0; io=ph9yvZbobwH52j_KAlYe",
    "Sec-Fetch-Dest" : " websocket",
    "Sec-Fetch-Mode" : " websocket",
    "Sec-Fetch-Site" : " same-site",
    "Pragma" : " no-cache",
    "Cache-Control" : " no-cache",
    "Upgrade" : " websocket",
}


@sio.event
def connect():
    print('connection established')


@sio.event
def my_message(data):
    print('message received with ', data)
    sio.emit('my response', {'response': 'my response'})


@sio.event
def disconnect():
    print('disconnected from server')


url = VPSWebSocketUrl.BG_DATAFEED()
# url = 'https://banggia.vps.com.vn/chung-khoan/VN30'
# sio.connect(url)
sio.connect(url, transports=['websocket'])
# sio.connect(url, headers=ws_headers, transports=['websocket'])
# sio.connect(url, headers=headers, transports=['websocket'])
print(sio.sid)
sio.wait()
