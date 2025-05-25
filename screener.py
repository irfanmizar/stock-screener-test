import yfinance as yf
import pandas as pd

def price_change_percentage(df):
    """
    Calculate the price change between the start and end of the period.
    """
    # window = df.loc[start:end]
    start_price = df["Close"].iloc[0]
    end_price = df["Close"].iloc[-1]
    return ((end_price - start_price) / start_price) * 100

def average_volume(ticker, start):
    """
    Calculate the average volume between the start and end of the period.
    1. Get the daily volume for each trading day in the past N days
    2. Compute the arithmetic mean of the daily volumes
    """
    current_ticker = yf.Ticker(ticker)
    last_N_days = current_ticker.history(
        period="5d",
        end=start,
        interval="1d"
    )
    avg_volume = last_N_days["Volume"].mean()
    return avg_volume

def volume(ticker, start, end):
    """
    Get the current volume for the ticker.
    """
    current_ticker = yf.Ticker(ticker)
    per_day_volume = current_ticker.history(
        start=start,
        end=end,
        interval="1d"
    )
    current_volume = per_day_volume["Volume"].mean()
    # current_volume = df["Volume"].sum()
    return current_volume

def relative_volume(ticker, start, end):
    """
    Calculate the relative volume for the ticker.
    1. Get the current volume
    2. Get the average volume for the past N days
    3. Compute the ratio of current volume to average volume
    """
    current_volume = volume(ticker, start, end)
    avg_volume = average_volume(ticker, start)
    return (current_volume / avg_volume)*100


def stock_data(df, ticker, passed, start, end):
    """
    Fetch the corresponding data for the ticker in the given period.
    """
    # start = pd.to_datetime(start)
    # end = pd.to_datetime(end)
    print(df.tail())
    price_change = price_change_percentage(df)
    avg_volume = average_volume(ticker, start)
    current_volume = volume(ticker, start, end)
    rel_volume = relative_volume(ticker, start, end)
    passed.append({
        "Ticker": ticker,
        "Name": yf.Ticker(ticker).info["longName"],
        "Price": round(df["Close"].iloc[-1], 2),
        "Price Change (%)": round(price_change, 2),
        "Average Volume": int(avg_volume),
        "Volume": int(current_volume),
        "Relative Volume (%)": round(rel_volume, 2)
    })
    

def run_screener(tickers, interval, prepost, start, end):
    """
    Run the stock screener on the given tickers.
    """
    passed = []
    for symbol in tickers:
        yf_sym = symbol.replace(".", "-")
        df     = yf.Ticker(yf_sym).history(
                    interval=interval,
                    prepost=prepost,
                    start=start,
                    end=end
                 )

        if df.empty:
            print(f"Ticker {symbol} has no data.")
            continue
        stock_data(df, yf_sym, passed, start, end)

    return pd.DataFrame(passed)


    # if not passed:
    #     return pd.DataFrame(columns=["Ticker", "Name", "Momentum", "Volume Spike"])
    # return pd.DataFrame(passed)