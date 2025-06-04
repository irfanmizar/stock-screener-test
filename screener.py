import yfinance as yf
import pandas as pd
from millify import millify as mf

def price_change_percentage(df):
    """
    Calculate the price change between the start and end of the period.
    """
    # window = df.loc[start:end]
    start_price = df["Close"].iloc[0]
    end_price = df["Close"].iloc[-1]
    return ((end_price - start_price) / start_price) * 100

def volume(ticker, start, end, interval):
    """
    Get the current volume for the ticker.
    """
    current_ticker = yf.Ticker(ticker)
    volume = current_ticker.history(
        start=start,
        end=end,
        interval=interval
    )
    current_volume = volume["Volume"].sum()
    return current_volume

def average_volume(ticker, start, end, interval):
    """
    Calculate the average volume between the start and end of the period.
    1. Get the daily volume for each trading day in the past N days
    2. Compute the arithmetic mean of the daily volumes
    """
    current_ticker = yf.Ticker(ticker)
    volume = current_ticker.history(
        start=start,
        end=end,
        interval="1d"
    )
    avg_volume = volume["Volume"].mean()
    return avg_volume

def relative_volume(current_volume, ticker, start, end, interval):
    """
    Calculate the relative volume for the ticker.
    """
    current_avg_volume = average_volume(ticker, start, end, interval)
    current_ticker = yf.Ticker(ticker)
    last_10_days = current_ticker.history(
        start=end-pd.Timedelta(days=10),  # 10 days before the end
        end=start,
        interval=interval
    )
    past_average_volume = last_10_days["Volume"].mean()
    return (current_avg_volume/past_average_volume)*100


def stock_data(df, ticker, passed, start, end, interval):
    """
    Fetch the corresponding data for the ticker in the given period.
    """
    print(df.tail())
    price_change = price_change_percentage(df)
    avg_volume = average_volume(ticker, start, end, interval)

    current_volume = volume(ticker, start, end, interval)

    rel_volume = relative_volume(current_volume, ticker, start, end, interval)
    passed.append({
        "Ticker": ticker,
        "Name": yf.Ticker(ticker).info["longName"],
        "Price": round(df["Close"].iloc[-1], 2),
        "Price Change (%)": round(price_change, 2),
        "Average Volume": mf(int(avg_volume), precision=2),
        "Volume": mf(int(current_volume), precision=2),
        "Relative Volume (%)": round(rel_volume, 2)
    })
    

def run_screener(tickers, interval, start, end):
    """
    Run the stock screener on the given tickers.
    """
    passed = []
    for symbol in tickers:
        yf_sym = symbol.replace(".", "-")
        df     = yf.Ticker(yf_sym).history(
                    interval=interval,
                    start=start,
                    end=end
                 )

        if df.empty:
            print(f"Ticker {symbol} has no data.")
            continue
        stock_data(df, yf_sym, passed, start, end, interval)

    return pd.DataFrame(passed)


    # if not passed:
    #     return pd.DataFrame(columns=["Ticker", "Name", "Momentum", "Volume Spike"])
    # return pd.DataFrame(passed)