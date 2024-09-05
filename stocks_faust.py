import logging
import faust
from stocks_models import StockPrice

logger = logging.getLogger('stocksflow').getChild(__name__)

app_name = "stocksflow-1"
app = faust.App(
                app_name,
                broker='kafka://127.0.0.1:9092',
                web_port=6071,
                # consumer_auto_offset_reset='latest',
                config_source={
                    "datadir": f"storage/{app_name}",
                })

# Define topics
v = "2"
stock_price_topic = app.topic(f'stock-price-2-{v}', value_type=StockPrice)
