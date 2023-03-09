

class VPSWebSocketUrl:
    @staticmethod
    def BG_DATAFEED():
        return "wss://bgdatafeed.vps.com.vn"


class VPSHttpEndpointUrl:
    @staticmethod
    def LIST_STOCK_TRADE(symbol):
        return f'https://bgapidatafeed.vps.com.vn/getliststocktrade/{symbol}'

    @staticmethod
    def LIST_STOCK_DATA(symbol):
        return f'https://bgapidatafeed.vps.com.vn/getliststockdata/{symbol}'
