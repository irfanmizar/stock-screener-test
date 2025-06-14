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

def volume(df):
    """
    Get the current volume for the ticker.
    """
    return df["Volume"].sum()

def average_volume(df, df_daily):
    """
    Calculate the average volume between the start and end of the period.
    """
    total_shares = df["Volume"].sum()

    if total_shares == 0:
        return 0
    
    num_days = df_daily.shape[0]

    if num_days == 0:
        return 0
    
    avg_volume = total_shares / num_days
    return avg_volume

def relative_volume(df, df_daily, df_lookback):
    """
    Calculate the relative volume for the ticker.
    """
    current_avg_volume = average_volume(df, df_daily)
    past_average_volume = df_lookback["Volume"].mean()
    return (current_avg_volume/past_average_volume)*100


def stock_data(df, df_daily, df_lookback, ticker, company_name, passed, start, end, interval):
    """
    Fetch the corresponding data for the ticker in the given period.
    """
    print(df.tail())
    price_change = price_change_percentage(df)
    avg_volume = average_volume(df, df_daily)
    current_volume = volume(df)
    rel_volume = relative_volume(df, df_daily, df_lookback)
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

        df_daily = yf.download(
            tickers=batch,
            start=start,
            end=end,
            interval="1d",
            group_by='ticker',
            auto_adjust=True,
            threads= True,
            progress=False  # Disable progress bar for cleaner output
        )

        df_lookback = yf.download(
            tickers=batch,
            start=(start - pd.Timedelta(days=10)).strftime("%Y-%m-%d"),
            end=start.strftime("%Y-%m-%d"),
            interval="1d",
            group_by='ticker',
            auto_adjust=True,
            threads= True,
            progress=False  # Disable progress bar for cleaner output
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

            stock_data(df_sym, df_dsym, df_lookback_sym, symbol, company_name, passed, start, end, interval)
    
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