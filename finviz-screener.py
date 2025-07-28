import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from millify import millify as mf

EXPORT_URL = "https://elite.finviz.com/quote_export.ashx?"

def batch_downloader(tickers, mode, start, end, auth_token):
    """
    Download stock data for a list of symbols in batches. This ensures that we do not hit the API rate limits
    """
    all_data = []
    batch_size = 10  # Adjust batch size as needed
    for i in range(0, len(tickers), batch_size):
        for sym in tickers[i:i + batch_size]:
            data = fetch_stock_data(sym, mode, start, end, auth_token)
            all_data.append(data)
    if not all_data:
        print("No data found for the specified tickers and date range.")
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

def fetch_stock_data(symbol, mode, start, end, auth_token):
    start_dt = start.date().strftime("%Y/%m/%d")
    end_dt = end.date().strftime("%Y/%m/%d")
    if mode=="daily":
        p = "d"
    else:
        p = "1m"

    req_url = EXPORT_URL + "t={symbol}&p={p}&auth={auth_token}"
    resp = requests.get(req_url)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text), parse_dates=["Date"])
    mask = (df["Date"] >= start_dt) & (df["Date"] <= end_dt)
    df = df.loc[mask].copy()
    df["Ticker"] = symbol
    return df

def compute_daily_metrics(df):
    results = []
    for sym, grp in df.groupby("Ticker"):
        grp = grp.sort_values("Date")
        first_close = grp["Close"].iloc[0]
        last_close  = grp["Close"].iloc[-1]
        pct_change  = ((last_close - first_close) / first_close * 100) if first_close else 0
        total_vol   = int(grp["Volume"].sum())
        avg_vol     = int(grp["Volume"].mean()) if len(grp)>0 else 0

        results.append({
            "Ticker":            sym,
            "Price Change (%)":  round(pct_change, 2),
            "Total Volume":      total_vol,
            "Average Volume":    avg_vol
        })

    return pd.DataFrame(results)

def run_screener(tickers, mode, start, end, auth_token):
    """
    Run the stock screener on the given tickers.
    start and end are datetime objects.
    mode can be "daily" or "intra".
    This function returns a DataFrame with the screener results.
    """
    if mode == "daily":
        df = batch_downloader(tickers, mode, start, end, auth_token)
        if df.empty:
            print("No data found for the specified date range.")
            return pd.DataFrame()
        return compute_daily_metrics(df)
    else:
        df_intra = batch_downloader(tickers, mode, start, end, auth_token)

    return df