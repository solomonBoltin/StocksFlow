import asyncio
import logging

from stocks_models import StockPrice
from stocks_data import StocksData
import stocks_fetch
from stocks_faust import app

logger = logging.getLogger('stocksflow')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.DEBUG)
app.logger.addHandler(logging.StreamHandler())

stocks_data = StocksData()

loop = asyncio.get_event_loop()


class StocksFlow:
    # monitoring interval will be the time between each stock price check
    # reset interval will be the time between each reset of the stock prices that will allow publishing the stock price
    # means we publish price every 1h or if the stock price is not updated for 24h
    MONITORING_INTERVAL_SEC = 60 * 60  # 1 hour
    RESET_INTERVAL_SEC = 24 * 60 * 60  # 24 hours

    stock_prices = {}
    reset_prices_task = None

    @classmethod
    async def on_new_stock_price(cls, stock_price: StockPrice):
        await stocks_data.add_stock_price(stock_price)

    @classmethod
    async def monitor_stocks(cls, interval_sec: int):
        while True:
            # Getting the full list of stocks
            stock_symbols = await stocks_data.get_full_stocks_list()
            # stock_symbols = ["GOOG", "AMZN", "AAPL", "MSFT", "TSLA"]

            # Fetching the last price of each stock
            logger.info(f"Monitoring stocks: {stock_symbols} ..")
            for symbol in stock_symbols:
                stock_price = stocks_fetch.fetch_stock_price(symbol)

                # Checking if the stock is valid and that it has changed from the last ping
                if stock_price is not None and cls.stock_prices.get(symbol) != stock_price.key:
                    cls.stock_prices[symbol] = stock_price.key
                    await cls.on_new_stock_price(stock_price)

            await asyncio.sleep(interval_sec)

    @classmethod
    async def reset_stock_prices(cls):
        while True:
            await asyncio.sleep(cls.RESET_INTERVAL_SEC)
            cls.stock_prices.clear()
            logger.info("Resetting stock_prices")

    @classmethod
    async def run(cls):
        logger.info("Starting App")
        cls.reset_prices_task = loop.create_task(cls.reset_stock_prices())
        await cls.monitor_stocks(cls.MONITORING_INTERVAL_SEC)

    @classmethod
    async def stop(cls):
        # await app.stop()
        await cls.reset_prices_task.cancel()
        await loop.shutdown_asyncgens()


if __name__ == "__main__":
    try:
        loop.run_until_complete(StocksFlow.run())
    except Exception as e:
        logger.error(f"Error in StocksFlow: {e}")
        loop.run_until_complete(StocksFlow.stop())
        raise e
    finally:
        logger.info("Finally closing loop")
        loop.run_until_complete(app.stop())
        loop.close()
