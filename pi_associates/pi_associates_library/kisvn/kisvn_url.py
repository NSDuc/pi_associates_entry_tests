
class KisvnEndpoint:
    MAX_REALTIME_TICKS_NUMBER_PER_REQUEST = 100

    @staticmethod
    def REALTIME_TICKS(symbol):
        return f"https://trading.kisvn.vn/rest/api/v2/market/symbol/{symbol}/quote"
