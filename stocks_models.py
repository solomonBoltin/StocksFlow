from dataclasses import dataclass
import faust


@dataclass
class StockPrice(faust.Record):
    symbol: str
    price: float
    datetime: str
    is_open: bool = None

    @property
    def key(self):
        return self.symbol + str(self.is_open) + str(self.price)
