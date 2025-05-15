import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
from filters.momentum import momentum_screener

def main():
    tickers = pd.read_csv("stock_tickers.csv")
    tickers = tickers["Ticker"].tolist()
    exports = []

    # for ticker in tickers:
    # tickers[0] tests the first ticker
    for ticker in tickers:
        yf_ticker = ticker.replace(".", "-")
        ticker_object = yf.Ticker(yf_ticker)
        data = ticker_object.history(period="1d", interval="1m", prepost=True)

        if momentum_screener(data):
            name = ticker_object.info.get("longName", ticker)
            exports.append({
                "Ticker": ticker,
                "Name": name,
                "Momentum": True,
            })
    
    export_df = pd.DataFrame(exports, columns=["Ticker", "Name", "Momentum"])
    print(export_df)
    # print(data.columns.tolist())
    # print(data.head())

if __name__ == "__main__":
    main()