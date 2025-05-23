import yfinance as yf
import pandas as pd
# from filters.momentum import momentum_screener
# from filters.volume_spike import volume_spike_screener

def price_change_percentage(df, start, end):
    """
    Calculate the price change between the start and end of the period.
    """
    # window = df.loc[start:end]
    start_price = df["Close"].iloc[0]
    end_price = df["Close"].iloc[-1]
    return ((end_price - start_price) / start_price) * 100

def average_volume(df, start, end):
    """
    Calculate the average volume between the start and end of the period.
    1. Get the daily volume for each trading day in the past N days
    2. Compute the arithmetic mean of the daily volumes
    """
    avg_volume = df["Volume"].mean()
    return avg_volume


def stock_data(df, ticker, passed, start, end):
    """
    Fetch the corresponding data for the ticker in the given period.
    """
    # start = pd.to_datetime(start)
    # end = pd.to_datetime(end)
    print(df.tail())
    price_change = price_change_percentage(df, start, end)
    avg_volume = average_volume(df, start, end)
    passed.append({
        "Ticker": ticker,
        "Name": yf.Ticker(ticker).info["longName"],
        "Price": round(df["Close"].iloc[-1], 2),
        "Price Change (%)": round(price_change, 2),
        "Average Volume": int(avg_volume)
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