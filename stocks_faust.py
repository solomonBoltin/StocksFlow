# App configuration
import logging

import faust

from data import StockPrice, Stock

logger = logging.getLogger('stocksflow').getChild(__name__)

app = faust.App('app1',
                broker='kafka://127.0.0.1:9092',
                web_port=6071,
                # consumer_auto_offset_reset='latest',
                )

v = "2"
# Define topics

stock_price_topic = app.topic(f'stock-price-1-{v}', value_type=StockPrice)  # Define topic
stocks_topic = app.topic(f'stocks-1-{v}', value_type=Stock)
stocks_table = app.Table(f'stocks-table-1-{v}', partitions=1, default=None, value_type=Stock)


@app.agent(stocks_topic)
async def timelines_table_builder_processor(stocks_stream: faust.Stream):
    async for stock in stocks_stream:
        logger.info(f"Adding stock to stocks table: {stock}")
        stock: Stock
        stocks_table[stock.symbol] = stock