
import influxdb_client, os, time
import pandas as pd
import yfinance as yf
from datetime import date, datetime
from influxdb_client import InfluxDBClient
from copy import deepcopy

LIVE_POOLING = True
TICKERS = ["EPOL", "WIG20.WA", "PLN=X", "EURPLN=X", "SPY"]
REF_DATE = "2023-10-10" # YYYY-mm-dd format required

#influxDB cfg
with open("api.key") as f:
    TOKEN = f.readline()
ORG = "pairview"
URL = "http://localhost:8086"

bucket="demo1"

client = influxdb_client.InfluxDBClient(url=url, token=TOKEN, org=org)
influx_writer = client.write_api()

today = date.today()
gap = today - date.fromisoformat(REF_DATE)
origin_d = today - 2*gap
fetch_start_date = origin_d.strftime('%Y-%m-%d')

hist_quotas = yf.download(TICKERS, interval = '1h', start=fetch_start_date)
hist_quotas = hist_quotas['Close']
hist_quotas = hist_quotas.reset_index()
hist_quotas['Timestamp'] = (hist_quotas['Datetime'] - pd.Timestamp("1970-01-01 00:00 +00:00")) // pd.Timedelta("1s")

ref_prices = hist_quotas[hist_quotas["Datetime"].dt.round('d') == REF_DATE]
ref_prices = ref_prices.mean()

for ticker in TICKERS:
    hist_quotas[f"{ticker}_ref_chg"] = hist_quotas[ticker] / ref_prices[ticker]

price_history_payload = []

message = {
    "measurement" : "ask prices",
    "tags" : {},
    "fields" : {}
}

# historical-absolute values
for index, row in hist_quotas.iterrows():
    point_entry = deepcopy(message)
    for ticker in TICKERS:
        point_entry['fields'][ticker] = row[ticker]
    point_entry['time'] = row['Timestamp']
    price_history_payload.append(point_entry)

# historical-relative values
message["measurement"] = "change vs ref"
for index, row in hist_quotas.iterrows():
    point_entry = deepcopy(message)
    for ticker in TICKERS:
        point_entry['fields'][ticker] = row[f"{ticker}_ref_chg"]
    point_entry['time'] = row['Timestamp']
    price_history_payload.append(point_entry)

influx_writer.write(bucket, org, price_history_payload, write_precision='s')
time.sleep(5)
print("historical data ready")

# pooling
message["tags"] = {"fetched_live" : True}

while LIVE_POOLING:

    live_payload = []
    for ticker in TICKERS:

        ask = yf.Ticker(ticker).info['ask']

        # live-absolute values
        point_entry = deepcopy(message)
        point_entry["measurement"] = "ask prices"
        point_entry["fields"][ticker] = ask
        live_payload.append(point_entry)

        # live-relative values
        point_entry = deepcopy(message)
        point_entry["measurement"] = "change vs ref"
        point_entry["fields"][ticker] = ask / ref_prices[ticker]
        live_payload.append(point_entry)
        
    influx_writer.write(bucket, org, live_payload, write_precision='s')
    print(f"Latest data fetched: {datetime.now()}", end="\r")
    time.sleep(10)
