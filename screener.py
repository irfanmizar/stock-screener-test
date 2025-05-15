import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
# Load environment variables from .env file

def main():
    tickers = pd.read_csv("stock_tickers.csv")
    tickers = tickers["Ticker"].tolist()

    # for ticker in tickers:
    ticker = yf.Ticker(tickers[0])
    data = ticker.history(period="1d", interval="1m", prepost=True)
    print(data.columns.tolist())
    print(data.head())

if __name__ == "__main__":
    main()