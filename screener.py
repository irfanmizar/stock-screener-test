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
        interval=interval
    )
    total_shares = volume["Volume"].sum()
    
    daily_bars = yf.Ticker(ticker).history(
        start=start,
        end=end,
        interval="1d"
    )
    num_days = len(daily_bars)
    if num_days == 0:
        return 0
    avg_volume = total_shares / num_days
    return avg_volume

def relative_volume(current_volume, ticker, start, end, interval):
    """
    Calculate the relative volume for the ticker.
    """
    current_avg_volume = average_volume(ticker, start, end, interval)
    current_ticker = yf.Ticker(ticker)
    last_10_days = current_ticker.history(
        start=start-pd.Timedelta(days=10),  # 10 days before the end
        end=start,
        interval="1d"
    )
    past_average_volume = last_10_days["Volume"].mean()
    return (current_avg_volume/past_average_volume)*100


def stock_data(df, ticker, company_name, passed, start, end, interval):
    """
    Fetch the corresponding data for the ticker in the given period.
    """
    print(df.tail())
    price_change = price_change_percentage(df)
    avg_volume = average_volume(ticker, start, end, interval)

    current_volume = volume(ticker, start, end, interval)

    rel_volume = relative_volume(current_volume, ticker, start, end, interval)
    # info = yf.Ticker(ticker).info
    passed.append({
        "Ticker": ticker,
        "Name": company_name,
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
    # for i in range(0, len(tickers), 400):
    #     batch = tickers[i:i+400]
    #     # yf_sym = [symbol.replace(".", "-") for symbol in batch]
    #     syms = [r['Ticker'] for r in batch]
    for i in range(0, len(tickers), 400):
        batch = tickers[i:i+400]
        # Convert tickers to the format yfinance expects
        # batch = [rec["Ticker"].replace(".", "-") for rec in batch]

        df_batch = yf.download(
            tickers=batch,
            start=start,
            end=end,
            interval=interval,
            group_by='ticker',
            auto_adjust=True,
            threads= True,
            progress=False  # Disable progress bar for cleaner output
        )

        for sym in batch:
            try:
                df_sym = df_batch[sym].dropna(subset=["Close"])
            except KeyError:
                print(f"Ticker {sym} has no data.")
                continue
            if df_sym.empty:
                print(f"Ticker {sym} has no data.")
                continue
            symbol = sym.replace("-", ".")

            stock_data(df_sym, symbol, symbol, passed, start, end, interval)
    
    return pd.DataFrame(passed)


    # if not passed:
    #     return pd.DataFrame(columns=["Ticker", "Name", "Momentum", "Volume Spike"])
    # return pd.DataFrame(passed)

# def run_screener(tickers, interval, start, end):
#     """
#     Run the stock screener on the given tickers.
#     """
#     passed = []
#     for symbol in tickers:
#         yf_sym = symbol.replace(".", "-")
#         df     = yf.Ticker(yf_sym).history(
#                     interval=interval,
#                     start=start,
#                     end=end
#                  )

#         if df.empty:
#             print(f"Ticker {symbol} has no data.")
#             continue
#         stock_data(df, yf_sym, passed, start, end, interval)

#     return pd.DataFrame(passed)