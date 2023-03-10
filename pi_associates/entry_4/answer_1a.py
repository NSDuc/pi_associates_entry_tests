from pi_associates_library.binance.binance_trade_scraper import BinanceTradeScraper, BinanceTradeAggregationScraper
from pprint import pprint
import pandas as pd


if __name__ == '__main__':
    symbol = 'manausdt'
    # symbol = 'btcusdt'

    trade_scraper = BinanceTradeScraper(symbol)
    trade_agg_scraper = BinanceTradeAggregationScraper(symbol)

    # print(pd.DataFrame(trade_scraper.get_trades()))
    for i in range(3):
        print('\n\n\n\n\n\n***********')
        agg = trade_agg_scraper.get_trade_aggregation()
        print(F"\t From {agg['f']} to {agg['l']}")
        print(F"\t price = {agg['p'], type(agg.p)}, quantity = {agg['q'], type(agg.q)}")
        print("="*50)
        df = pd.DataFrame(trade_scraper.get_trades())
        df['price'] = df['price'].astype(float)
        df['qty'] = df['qty'].astype(float)
        df['quoteQty'] = df['quoteQty'].astype(float)

        print(df.info())
        df2 = df[(agg['f'] <= df['id']) & (df['id'] <= agg['l'])]
        df_sum_qty = df2['qty'].sum()
        df_avg_price = df2['quoteQty'].sum() / df_sum_qty
        print(type(df_sum_qty), df_sum_qty)
        print(type(agg['q']), agg['q'])
        print(f'sum quantity {df_sum_qty}', df_sum_qty == agg['q'])
        print(f'sum quantity {df_avg_price}', df_avg_price == agg['p'])
        print(df2)

