import logging
from typing import Optional
from datetime import datetime
import pytz
import yfinance as yf

from data import StockPrice

logger = logging.getLogger("stocksflow").getChild(__name__)

invalid_stocks = []


def fetch_stock_price(symbol: str) -> Optional[StockPrice]:
    if symbol in invalid_stocks:
        return None

    try:
        ticker = yf.Ticker(symbol)

        # checking if the stock is valid
        if ticker.info.get("shortName") is None:
            invalid_stocks.append(symbol)
            return None

        # fetching the last price of the stock and creating a StockPrice object
        last_price = ticker.fast_info['last_price']
        is_open = is_market_open(ticker)
        stock_price = StockPrice(
            symbol=symbol,
            price=last_price,
            datetime=datetime.now().isoformat(),
            is_open=is_open
        )
        logger.info(f"Fetched stock price for {symbol}: {stock_price}")
        return stock_price

    except Exception as e:
        logger.error(f"Error fetching stock price for {symbol}: {e}")
        return None


def is_market_open(tk: yf.Ticker):
    history_metadata = tk.history_metadata

    # Check if history metadata is available
    if not history_metadata or not history_metadata.get("currentTradingPeriod"):
        return None

    timezone_name = history_metadata["exchangeTimezoneName"]
    timezone = pytz.timezone(timezone_name)

    current_trading_period = history_metadata["currentTradingPeriod"]

    # Get current time in the same timezone
    # now = datetime.fromisoformat("2024-09-03T11:54:25.321180-04:00")
    now = datetime.now(timezone)

    # Extract market open and close times
    regular_period = current_trading_period['regular']
    open_time_unix = regular_period['start']
    close_time_unix = regular_period['end']

    # Convert Unix timestamps to datetime
    market_open = datetime.fromtimestamp(open_time_unix, timezone)
    market_close = datetime.fromtimestamp(close_time_unix, timezone)

    # Check if current time is within the market open period
    return market_open <= now <= market_close


def ts_fetch_stock_price():
    ap = fetch_stock_price("AAPL")
    assert ap is not None
    assert ap.symbol == "AAPL"
    assert ap.price > 0
    assert ap.datetime is not None
    assert ap.is_open is not None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print(fetch_stock_price("AAPL"))


