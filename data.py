from dataclasses import dataclass

import faust


@dataclass
class StockPrice(faust.Record):
    symbol: str
    price: float
    datetime: str


@dataclass
class Stock(faust.Record):
    symbol: str
    name: str
