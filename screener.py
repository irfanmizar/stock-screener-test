import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import argparse
from filters.momentum import momentum_screener
from filters.volume_spike import volume_spike_screener

# this function is used to parse the input arguments
def parse_args():
    parse = argparse.ArgumentParser(description="Screener for stocks")
    parse.add_argument(
        "--tickers",
        type=str,
        default="stock_tickers.csv",
        help="Path to the CSV file containing stock tickers",
    )
    # the default argument for period is 1d which is the current day
    parse.add_argument(
        "--period",
        type=str,
        default="1d",
        help="yfinance period parameter",
    )
    # the default argument for interval is 1m so the gaps between each data point is 1 minute
    parse.add_argument(
        "--interval",
        type=str,
        default="1m",
        help="yfinance interval parameter",
    )
    # prepost includes pre and post market data
    parse.add_argument(
        "--prepost",
        type=bool,
        default=True,
        help="Include pre and post market data",
    )
    # the default argument for threshold is 0.75 which is 0.75% change in price between the start and end of the period
    parse.add_argument(
        "--momentum_threshold",
        type=float,
        default=0.75,
        help="Momentum threshold percentage",
    )
    # the default argument for volume_threshold is 90.0 which is 90% change in volume between the start and end of the period
    parse.add_argument(
        "--volume_threshold",
        type=float,
        default=90.0,
        help="Volume spike threshold percentage",
    )
    return parse.parse_args()


def main():
    # parse input arguments
    args = parse_args()

    # get the tickers from CSV file and convert to list
    tickers = pd.read_csv(args.tickers)
    tickers = tickers["Ticker"].tolist()

    # output list
    exports = []

    for ticker in tickers:
        yf_ticker = ticker.replace(".", "-")
        ticker_object = yf.Ticker(yf_ticker)
        data = ticker_object.history(
            period=args.period,
            interval=args.interval,
            prepost=args.prepost,
        )
        # print(data.columns.tolist())
        # print(data)
        # break

        if momentum_screener(data, args.momentum_threshold) and volume_spike_screener(data, args.volume_threshold):
            name = ticker_object.info.get("longName", ticker)
            exports.append({
                "Ticker": ticker,
                "Name": name,
                "Momentum": True,
            })
    
    export_df = pd.DataFrame(exports, columns=["Ticker", "Name", "Momentum"])
    print(export_df)

if __name__ == "__main__":
    main()