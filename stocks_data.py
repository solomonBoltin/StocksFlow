import logging
from dataclasses import dataclass

from faust import TopicT, Table
from api.client import NewsProcessorClient
import faust
import asyncio

from stocks_faust import stock_price_topic
from stocks_models import StockPrice

logger = logging.getLogger('stocksflow').getChild(__name__)


class StocksData:

    def __init__(self, stock_price_topic: TopicT = None, stocks_table: Table = None):
        self.stock_price_topic = stock_price_topic
        self.stocks_table = stocks_table
        self.newsprocessor_client = NewsProcessorClient()

    async def get_full_stocks_list(self):
        # Get full list of stocks from Kafka store
        return await self.newsprocessor_client.get_stocks()

    async def add_stock_price(self, stock_price: StockPrice):
        # Publish stock price to Kafka
        logger.info(f"Publishing stock price {stock_price}")
        await stock_price_topic.send(key=stock_price.symbol, value=stock_price)


async def main():
    sd = StocksData()
    r = await sd.get_full_stocks_list()

    print(r)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
