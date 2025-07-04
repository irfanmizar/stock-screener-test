import yfinance as yf
import pandas as pd
from millify import millify as mf

def price_change_percentage(df_daily):
    """
    Calculate the percentage change in price from the first to the last day."""
    if df_daily.empty:
        return 0
    start_price = df_daily["Close"].iloc[0]
    end_price = df_daily["Close"].iloc[-1]
    return ((end_price - start_price) / start_price) * 100

def price_change_absolute(df_daily):
    """
    Calculate the absolute price change from the first to the last day.
    """
    if df_daily.empty:
        return 0
    start_price = df_daily["Close"].iloc[0]
    end_price = df_daily["Close"].iloc[-1]
    if pd.isna(start_price) or pd.isna(end_price):
        return 0
    return end_price - start_price

def price_change_percentage_opentoclose(df):
    """
    Calculate the percentage change in price from the first open to the last close.
    """
    if df.empty:
        return 0
    start_price = df["Open"].iloc[0]
    end_price = df["Close"].iloc[-1]
    return ((end_price - start_price) / start_price) * 100

def volume(df_daily):
    if df_daily.empty:
        return 0
     #print(f"Daily Volume: {df_daily['Volume']}")
    current_volume = df_daily["Volume"].sum() - df_daily["Volume"].iloc[0]  # Subtract the first volume to get the change
    if pd.isna(current_volume):
        return 0
    return current_volume

def average_volume_daily(df_daily):
    if df_daily.empty:
        return 0
    daily = df_daily["Volume"].sum() - df_daily["Volume"].iloc[0]
    row_count = df_daily.shape[0] - 1
    average_daily_volume = daily / row_count if row_count > 0 else 0
    if pd.isna(average_daily_volume):
        return 0
    return average_daily_volume

def average_volume(df_lookback):
    """
    Calculate the average volume between the start and end of the period.
    """
    if df_lookback.empty:
        return 0
    avg = df_lookback["Volume"].mean()
    if pd.isna(avg):
        return 0
    return avg

def relative_volume(avg_volume, current_avg_volume):
    """
    Calculate the relative volume for the ticker.
    """
    return (current_avg_volume/avg_volume) if avg_volume > 0 else 0


def stock_data(df, df_daily, df_lookback, ticker, company_name, passed, start, end, interval):
    """
    Fetch the corresponding data for the ticker in the given period.
    """
    # print(df.tail())
    price_change = price_change_percentage(df_daily)
    price_change_abs = price_change_absolute(df_daily)
    current_volume = volume(df_daily)
    daily_volume = average_volume_daily(df_daily)
    avg_volume = average_volume(df_lookback)
    rel_volume = relative_volume(avg_volume, daily_volume)
    
    # info = yf.Ticker(ticker).info
    passed.append({
        "Ticker": ticker,
        # "Name": company_name,
        "Price": round(df_daily["Close"].iloc[-1], 2),
        "PC (%)": round(price_change, 2),
        "PC ": round(price_change_abs, 2),
        "PC2 (%)": round(price_change_percentage_opentoclose(df), 2),
        "Total Volume": int(current_volume),
        "Average Daily Volume": int(daily_volume),
        "Average Volume": int(avg_volume),
        "Relative Volume ": round(rel_volume, 2)
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
            start=(start - pd.Timedelta(days=90)).strftime("%Y-%m-%d"),
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