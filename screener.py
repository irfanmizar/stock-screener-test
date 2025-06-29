import yfinance as yf
import pandas as pd
from millify import millify as mf

def price_change_percentage(df, df_lookback):
    start_price = df_lookback["Close"].iloc[-1]
    end_price = df["Close"].iloc[-1]
    # print(f"Start Price: {start_price}, End Price: {end_price}")
    return ((end_price - start_price) / start_price) * 100

def price_change_percentage_opentoclose(df):
    start_price = df["Open"].iloc[0]
    end_price = df["Close"].iloc[-1]
    return ((end_price - start_price) / start_price) * 100

def volume(df):
    return df["Volume"].sum()

def volume_daily(df_daily):
    if df_daily.empty:
        return 0
    return df_daily["Volume"].sum()

def average_volume(df, df_daily):
    """
    Calculate the average volume between the start and end of the period.
    """
    # total_shares = df["Volume"].sum()

    # if total_shares == 0:
    #     return 0
    
    # num_days = df_daily.shape[0]

    # if num_days == 0:
    #     return 0
    
    # avg_volume = total_shares / num_days
    # return avg_volume
    if df_daily.empty or df.empty:
        return 0
    avg = df_daily["Volume"].mean()
    if pd.isna(avg):
        return 0
    return avg

def relative_volume(df_lookback, avg_volume):
    """
    Calculate the relative volume for the ticker.
    """
    past_average_volume = df_lookback["Volume"].mean()
    return (avg_volume/past_average_volume)*100


def stock_data(df, df_daily, df_lookback, ticker, company_name, passed, start, end, interval):
    """
    Fetch the corresponding data for the ticker in the given period.
    """
    # print(df.tail())
    price_change = price_change_percentage(df, df_lookback)
    avg_volume = average_volume(df, df_daily)
    current_volume = volume(df)
    rel_volume = relative_volume(df_lookback, avg_volume)
    # info = yf.Ticker(ticker).info
    passed.append({
        "Ticker": ticker,
        # "Name": company_name,
        "Price": round(df["Close"].iloc[-1], 2),
        "Price Change (%)": round(price_change, 2),
        "Price Change (Open to Close) (%)": round(price_change_percentage_opentoclose(df), 2),
        "Average Volume": int(avg_volume),
        "Daily Volume": int(volume_daily(df_daily)),
        "Volume": int(current_volume),
        "Relative Volume (%)": round(rel_volume, 2)
    })
    

def run_screener(tickers, interval, start, end, num_days, prepost):
    """
    Run the stock screener on the given tickers.
    """
    passed = []
    for i in range(0, len(tickers), 400):
        batch = tickers[i:i+400]

        df_batch = yf.download(
        tickers=batch,
        start=start,
        end=end,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
        prepost=prepost  # Disable pre/post market data for cleaner output
        )

        df_daily = yf.download(
            tickers=batch,
            start=start,
            end=end,
            interval="1d",
            group_by='ticker',
            auto_adjust=False,
            threads= True,
            progress=False,  # Disable progress bar for cleaner output
            prepost=False  # Disable pre/post market data for cleaner output
        )

        df_lookback = yf.download(
            tickers=batch,
            start=(start - pd.Timedelta(days=10)).strftime("%Y-%m-%d"),
            end=start.strftime("%Y-%m-%d"),
            interval="1d",
            group_by='ticker',
            auto_adjust=False,
            threads= True,
            progress=False,  # Disable progress bar for cleaner output
            prepost=False  # Disable pre/post market data for cleaner output
        )

        for sym in batch:
            symbol = sym.replace("-", ".")
            company_name = sym['Company'] if isinstance(sym, dict) and 'Company' in sym else sym
            # Check if the ticker exists in the downloaded data
            if sym not in df_batch.columns:
                print(f"Ticker {sym} has no data.")
                continue
            try:
                df_sym = df_batch[sym].dropna(subset=["Close"])
                df_dsym = df_daily[sym]
                df_lookback_sym = df_lookback[sym]
            except KeyError:
                print(f"Ticker {sym} has no data.")
                continue
            if df_sym.empty or df_dsym.empty or df_lookback_sym.empty:
                print(f"Ticker {sym} has no data.")
                continue
            symbol = sym.replace("-", ".")
            print(sym)
            print(df_batch[sym].head(10))
            print(df_batch[sym].tail(5))

            stock_data(df_sym, df_dsym, df_lookback_sym, symbol, company_name, passed, start, end, interval)
    
    return pd.DataFrame(passed)