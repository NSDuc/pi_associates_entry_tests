import asyncio

from pi_associates_library.binance.binance_orderbook_storage import BinanceOrderbookStorage, KafkaBinanceOrderbookStorage
from pi_associates_library.binance.binance_orderbook_scraper import BinanceOrderbookScraper
from pi_associates_library.job_runner import ProcessJobRunner, ThreadJobRunner, JobRunner
from pi_associates_library.config_loader import DotenvConfigLoader


if __name__ == '__main__':
    job_runner: JobRunner = ThreadJobRunner()
    config: dict = DotenvConfigLoader().load_config()

    symbols = config.get('PI_ASSOCIATES_BINANCE_SCRAPED_SYMBOLS').split(',')

    # for symbol in symbols:
    #     orderbook_storage = KafkaBinanceOrderbookStorage(symbol)
    #     orderbook_scraper = BinanceOrderbookScraper(symbol, orderbook_storage)
    #
    #     job_runner.create_job(job_name=symbol,
    #                           target=orderbook_scraper.scrape,
    #                           args=[])
    #
    # job_runner.run_until_complete()
    symbol = symbols[0]
    orderbook_storage = KafkaBinanceOrderbookStorage(symbol)
    orderbook_scraper = BinanceOrderbookScraper(symbol, orderbook_storage)
    asyncio.get_event_loop().run_until_complete(orderbook_scraper.scrape())
