from datetime import datetime
from binance_data_downloader import download_query, schema

symbols = ["BTCUSDT", "ETHUSDT"]
start = datetime(2019, 10, 1)          # USD-M futures exist from ~Oct 2019 onward
end   = datetime(2026, 2, 2)


# funding rate (USD-M futures)
q2 = schema.QuerySchema(
    market="um",
    endpoint="fundingRate",
    symbol=symbols,
    start=start,
    end=end,
    verify_checksum=True
)
download_query(config=q2, directory="./downloads")
