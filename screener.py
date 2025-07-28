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

def volume(df, df_daily, intraday=False):
    if df_daily.empty:
        return 0
     #print(f"Daily Volume: {df_daily['Volume']}")
    current_volume = df_daily["Volume"].sum() - df_daily["Volume"].iloc[0]  # Subtract the first volume to get the change
    print("Head: ", df.head(3))
    print("Tail: ", df.tail(3))
        

    # print("Test Volume: ", test_vol)
    print("Current Volume: ", current_volume)
    
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

def slice_window(df_intraday, ticker, start_dt, end_dt):
    """
    Slice the intraday data for the ticker between start_dt and end_dt.
    """
    if df_intraday.empty:
        return pd.DataFrame()
    try:
        new_df = df_intraday[ticker]
    except KeyError:
        print(f"Ticker {ticker} not found in the DataFrame.")
        return pd.DataFrame()
    mask = (new_df.index >= start_dt) & (new_df.index <= end_dt)
    df_intraday = new_df.loc[mask]
    if df_intraday.empty:
        return pd.DataFrame()
    return df_intraday

def compute_metrics(df_slice, df_3m_daily, df_3m_intraday):
    print(df_slice.head(3))
    print(df_slice.tail(3))
    p0 = df_slice["Open"].iloc[0]
    p1 = df_slice["Close"].iloc[-1]
    p_change = (p1 - p0) / p0 * 100 if p0 != 0 else 0
    p_change_abs = p1 - p0
    if pd.isna(p_change) or pd.isna(p_change_abs):
        p_change = 0
        p_change_abs = 0

    total_volume = df_slice["Volume"].sum() if not df_slice.empty else 0
    if pd.isna(total_volume):
        total_volume = 0
    avg_daily_volume = df_3m_daily["Volume"].mean() if not df_3m_daily.empty else 0
    rel_vol_eod = total_volume / avg_daily_volume if avg_daily_volume > 0 else 0

    return p_change, p_change_abs, total_volume, avg_daily_volume, rel_vol_eod


def stock_data(df, df_daily, df_lookback, ticker, company_name, passed, start, end, interval):
    """
    Fetch the corresponding data for the ticker in the given period.
    """
    # print(df.tail())
    price_change = price_change_percentage(df_daily)
    price_change_abs = price_change_absolute(df_daily)
    current_volume = volume(df, df_daily)
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
    for i in range(0, len(tickers), 200):
        batch = tickers[i:i+200]

        df_batch = yf.download(
        tickers=batch,
        start=start,
        end=end,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
        prepost=prepost
        )

        df_batch.index = df_batch.index.tz_convert(None)

        df_daily = yf.download(
            tickers=batch,
            start=start,
            end=end,
            interval="1d",
            group_by='ticker',
            auto_adjust=False,
            threads= True,
            progress=False,
            prepost=False
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

        for ticker in batch:
            symbol = ticker.replace("-", ".")
            company_name = ticker['Company'] if isinstance(ticker, dict) and 'Company' in ticker else ticker

            print(f"Processing ticker: {symbol} ({company_name})")
            df_slice = slice_window(df_batch, symbol, start, end)
            if df_slice.empty:
                print(f"No data for ticker {symbol} in the specified date range.")
                continue

            df_3m_daily = df_daily[symbol] if symbol in df_daily.columns else pd.DataFrame()
            df_3m_intraday = df_batch[symbol] if symbol in df_batch.columns else pd.DataFrame()

            p_change, p_change_abs, total_volume, avg_daily_volume, rel_vol_eod = compute_metrics(df_slice, df_3m_daily, None)

            passed.append({
                "Ticker": symbol,
                "Name": company_name,
                "Price": round(df_slice["Close"].iloc[-1], 2),
                "PC (%)": round(p_change, 2),
                "PC ": round(p_change_abs, 2),
                "PC2 (%)": round(price_change_percentage_opentoclose(df_slice), 2),
                "Total Volume": int(total_volume),
                "Average Daily Volume": int(avg_daily_volume),
                "Average Volume": int(avg_daily_volume) if not df_lookback.empty else 0,
                "Relative Volume ": round(rel_vol_eod, 2)
            })


        # for sym in batch:
        #     symbol = sym.replace("-", ".")
        #     company_name = sym['Company'] if isinstance(sym, dict) and 'Company' in sym else sym
        #     # Check if the ticker exists in the downloaded data
        #     if sym not in df_batch.columns:
        #         print(f"Ticker {sym} has no data.")
        #         continue
        #     try:
        #         df_sym = df_batch[sym].dropna(subset=["Close"])
        #         df_dsym = df_daily[sym]
        #         df_lookback_sym = df_lookback[sym]
        #     except KeyError:
        #         print(f"Ticker {sym} has no data.")
        #         continue
        #     if df_sym.empty or df_dsym.empty or df_lookback_sym.empty:
        #         print(f"Ticker {sym} has no data.")
        #         continue
        #     symbol = sym.replace("-", ".")
        #     print(sym)
        #     # print(df_batch[sym].head(10))
        #     # print(df_batch[sym].tail(5))

        #     stock_data(df_sym, df_dsym, df_lookback_sym, symbol, company_name, passed, start, end, interval)
    
    return pd.DataFrame(passed)