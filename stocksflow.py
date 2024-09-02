import asyncio
import logging
import datetime

from data import StockPrice
from stocks_data import StocksData
import stocks_fetch
from stocks_faust import app, stock_price_topic, stocks_table

logger = logging.getLogger('stocksflow')
stocks_data = StocksData(stock_price_topic, stocks_table)

loop = asyncio.get_event_loop()


class StocksFlow:
    stock_prices = {}

    @classmethod
    async def on_new_stock_price(cls, stock: str, price: float, timestamp: str):
        await stocks_data.add_stock_price(StockPrice(
            symbol=stock,
            price=price,
            datetime=timestamp
        ))

    @classmethod
    async def monitor_stocks(cls, interval_sec: int):
        while True:
            stocks = await stocks_data.get_full_stocks_list()
            logger.info(f"Monitoring stocks: {stocks}")
            # stocks = ["AAPL", "GOOGL", "AMZN"]
            for stock in stocks:
                price = stocks_fetch.fetch_stock_last_price(stock)
                if price is not None and price != cls.stock_prices.get(stock):
                    cls.stock_prices[stock] = price
                    await cls.on_new_stock_price(stock, price, datetime.datetime.now().isoformat())
            await asyncio.sleep(interval_sec)

    @classmethod
    async def run(cls):
        logger.info("Starting App")
        app.loop = loop
        await app.start()

        logger.info("Monitoring stocks")
        await cls.monitor_stocks(60 * 1)

    @classmethod
    async def stop(cls):
        await app.stop()
        await loop.shutdown_asyncgens()


if __name__ == "__main__":
    try:
        app.logger.setLevel(logging.DEBUG)
        logger.setLevel(logging.INFO)

        app.logger.addHandler(logging.StreamHandler())
        logger.addHandler(logging.StreamHandler())
        loop.run_until_complete(StocksFlow.run())
    except Exception as e:
        logger.error(f"Error in StocksFlow: {e}")
        loop.run_until_complete(StocksFlow.stop())
        raise e
    finally:
        logger.info("Finally closing loop")
        loop.run_until_complete(app.stop())
        loop.close()
