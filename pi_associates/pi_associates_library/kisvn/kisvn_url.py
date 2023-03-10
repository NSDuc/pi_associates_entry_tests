
class KisvnEndpoint:
    @staticmethod
    def REALTIME_TICKS(symbol):
        return f"https://trading.kisvn.vn/rest/api/v2/market/symbol/{symbol}/quote"
