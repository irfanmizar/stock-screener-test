import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import argparse
from screener import run_screener

# this function is used to parse the input arguments
def parse_args():
    parse = argparse.ArgumentParser(description="Screener for stocks")
    parse.add_argument(
        "--tickers",
        type=str,
        default="stock_tickers.csv",
        help="Path to the CSV file containing stock tickers",
    )
    # # the default argument for period is 1d which is the current day
    # parse.add_argument(
    #     "--period",
    #     type=str,
    #     default="1d",
    #     help="yfinance period parameter",
    # )
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
        default=False,
        help="Include pre and post market data",
    )

    parse.add_argument(
        "--start",
        type=str,
        default=datetime.date(2025, 5, 1).strftime("%Y-%m-%d"),
        help="Start date for the data in YYYY-MM-DD format",
    )

    parse.add_argument(
        "--end",
        type=str,
        default=datetime.date(2025, 5, 7).strftime("%Y-%m-%d"),
        help="End date for the data in YYYY-MM-DD format",
    )
    return parse.parse_args()


def main():
    # parse input arguments
    args = parse_args()

    # get the tickers from CSV file and convert to list
    # tickers = pd.read_csv(args.tickers)
    # tickers = tickers["Ticker"].tolist()
    tickers = ["MMM", "AAPL", "MSFT", "GOOGL", "NVDA", "AMD", "CFSB"]

    # price change (%)
    # avg volume
    # relative volume

    export_df = run_screener(
        tickers,
        interval=args.interval,
        prepost=args.prepost,
        start=args.start,
        end=args.end,
    )
    print(export_df)
    
if __name__ == "__main__":
    main()