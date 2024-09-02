import logging

import yfinance as yf

logger = logging.getLogger("stocksflow").getChild(__name__)

invalid_stocks = []


def fetch_stock_last_price(stock: str):
    if stock in invalid_stocks:
        return None
    try:
        ticker = yf.Ticker(stock)
        if ticker.info.get("shortName") is None:
            invalid_stocks.append(stock)
            return None
        last_price = ticker.fast_info['last_price']
        logger.info(f"Last price of {stock}: {last_price}")
        return last_price
    except Exception as e:
        logger.error(f"Error fetching stock price for {stock}: {e}")
        return None


#
# t = yf.Ticker("LUMI.TA")
# print(t.info.get("shortName"))
