
import pandas as pd

# pip install yfinance --upgrade --no-cache-dir
import yfinance as yf

msft = yf.Ticker("MSFT")

# get all stock info
data = yf.download("SPY AAPL", period="1mo")

print(data.head(5))