
import influxdb_client, os, time
import pandas as pd
import yfinance as yf
from datetime import date
from influxdb_client import InfluxDBClient
from copy import deepcopy

LIVE_POOLING = False
tickers = ["EPOL", "WIG20.WA", "PLN=X", "EURPLN=X"]
ref_date = "2023-10-10" # YYYY-mm-dd format required

#influxDB cfg
token = "k-iLAgyYnB8wDbTO5NXKeXw0Db3EQjHpwFeeCEVdCKo7pDrAzCOExEqj1JaarmkVDO7chKj-2KEudwwzPm0Zhg=="
org = "pairview"
url = "http://localhost:8086"
bucket="testo8"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
influx_writer = client.write_api()

today = date.today()
gap = today - date.fromisoformat(ref_date)
origin_d = today - 2*gap
fetch_start_date = origin_d.strftime('%Y-%m-%d')

hist_quotas = yf.download(tickers, interval = '1h', start=fetch_start_date)
hist_quotas = hist_quotas['Close']
hist_quotas = hist_quotas.reset_index()
hist_quotas['Timestamp'] = (hist_quotas['Datetime'] - pd.Timestamp("1970-01-01 00:00 +00:00")) // pd.Timedelta("1s")

ref_prices = hist_quotas[hist_quotas["Datetime"].dt.round('d') == ref_date]
ref_prices = ref_prices.mean()

for ticker in tickers:
    hist_quotas[f"{ticker}_ref_chg"] = hist_quotas[ticker] / ref_prices[ticker]

price_history_payload = []

message = {
    "measurement" : "ask prices",
    "tags" : {},
    "fields" : {}
}
# absolute values
for index, row in hist_quotas.iterrows():
    point_entry = dict(message)
    for ticker in tickers:
        point_entry['fields'][ticker] = row[ticker]
    point_entry['time'] = row['Timestamp']
    price_history_payload.append(deepcopy(point_entry))

message = {
    "measurement" : "change vs ref",
    "tags" : {},
    "fields" : {}
}
# relative values
for index, row in hist_quotas.iterrows():
    point_entry = dict(message)
    for ticker in tickers:
        point_entry['fields'][ticker] = row[f"{ticker}_ref_chg"]
    point_entry['time'] = row['Timestamp']
    price_history_payload.append(deepcopy(point_entry))

influx_writer.write(bucket, org, price_history_payload, write_precision='s')
time.sleep(5)
print("historical data ready")

# pooling
while LIVE_POOLING:

    for pair in tickers:

        tick = yf.Ticker(pair) #todo single request (move out of loop)
        ask = tick.info['ask']
        message["fields"][pair + '_ask'] = ask

    influx_writer.write(bucket, org, message)
    time.sleep(10)
