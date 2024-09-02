# publish stock price
# get latest price from kf store
# get all stocks list
import logging

from faust import TopicT, Table

from data import StockPrice

logger = logging.getLogger('stocksflow').getChild(__name__)


class StocksData:

    def __init__(self, stock_price_topic: TopicT, stocks_table: Table):
        self.stock_price_topic = stock_price_topic
        self.stocks_table = stocks_table

    async def get_full_stocks_list(self):
        # Get full list of stocks from Kafka store
        return list(self.stocks_table.keys())

    async def add_stock_price(self, stock_price: StockPrice):
        # Publish stock price to Kafka
        logger.info(f"Publishing stock price {stock_price}")
        await self.stock_price_topic.send(key=stock_price.symbol, value=stock_price)
