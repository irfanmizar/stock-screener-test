import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta, time

EXPORT_URL = "https://elite.finviz.com/quote_export.ashx?"
MARKET_OPEN  = time(9, 30)
MARKET_CLOSE = time(16, 0)

def batch_downloader(tickers, mode, start, end, auth_token):
    """
    Download stock data for a list of symbols in batches. This ensures that we do not hit the API rate limits
    """
    all_data = []
    batch_size = 1  # Adjust batch size as needed
    for i in range(0, len(tickers), batch_size):
        for sym in tickers[i:i + batch_size]:
            data = fetch_stock_data(sym, mode, start, end, auth_token)
            all_data.append(data)
    if not all_data:
        print("No data found for the specified tickers and date range.")
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

def fetch_stock_data(symbol, mode, start, end, auth_token):
    # start_dt = start.date().strftime("%Y/%m/%d")
    # end_dt = end.date().strftime("%Y/%m/%d")
    # if mode=="daily":
    #     p = "d"
    # else:
    #     p = "i1"

    # req_url = EXPORT_URL + "t={symbol}&p={p}&auth={auth_token}"
    # resp = requests.get(req_url)
    # resp.raise_for_status()
    p = "d" if mode == "daily" else "i1"
    url = f"{EXPORT_URL}t={symbol}&p={p}&auth={auth_token}"
    resp = requests.get(url); resp.raise_for_status()

    # infer_datetime_format lets pandas parse both "MM/DD/YYYY" and "MM/DD/YYYY hh:mm AM/PM"
    df = pd.read_csv(
        StringIO(resp.text),
        parse_dates=["Date"],
    )
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    if mode == "daily":
        mask = (
            (df["Date"].dt.date >= start.date()) &
            (df["Date"].dt.date <= end.date())
        )
    else:
        mask = (
            (df["Date"] >= start) &
            (df["Date"] <= end)
        )

    df = df.loc[mask].copy()
    df["Ticker"] = symbol
    return df

def compute_daily_metrics(df, baseline_avg_daily):  # ➕ added second arg
    rows = []
    for sym, grp in df.groupby("Ticker"):
        grp = grp.sort_values("Date")
        # price change
        p0 = grp["Close"].iloc[0]
        p1 = grp["Close"].iloc[-1]
        pct = ((p1 - p0)/p0*100) if p0 else 0

        # volume metrics over the window
        vols = grp["Volume"]
        total_vol   = int(vols.sum()) - int(grp["Volume"].iloc[0])
        avg_bar_vol = total_vol/(len(grp)-1)

        # relative vs baseline daily
        base = baseline_avg_daily.get(sym, avg_bar_vol)
        rel  = round(avg_bar_vol / base, 2) if base else None

        rows.append({
            "Ticker":           sym,
            "Price Change (%)": round(pct, 2),
            "Total Volume":     total_vol,
            "Average Volume":   avg_bar_vol,
            "Relative Volume":  rel        # ➕ new column
        })
    return pd.DataFrame(rows)

def compute_intraday_metrics(df, baseline_avg_daily):  # ➕ added second arg
    rows = []
    for sym, grp in df.groupby("Ticker"):
        grp = grp.sort_values("Date")
        # price change using intraday bars
        p0 = grp["Open"].iloc[0]
        p1 = grp["Close"].iloc[-1]
        pct = ((p1 - p0)/p0*100) if p0 else 0

        # minute-bar volume metrics
        vols = grp["Volume"]
        total_vol   = int(vols.sum())
        avg_bar_vol = int(vols.mean())

        # derive a baseline per-minute volume
        base_daily  = baseline_avg_daily.get(sym, None)
        if base_daily:
            base_minute = base_daily / 390  # ~390 trading minutes/day
            rel = round(avg_bar_vol / base_minute, 2)
        else:
            rel = None

        rows.append({
            "Ticker":           sym,
            "Price Change (%)": round(pct, 2),
            "Total Volume":     total_vol,
            "Average Volume":   avg_bar_vol,
            "Relative Volume":  rel        # ➕ new column
        })
    return pd.DataFrame(rows)

def run_screener(tickers, mode, start, end, auth_token, lookback_days=90):
    """
    - Computes a 90-day baseline average daily volume (per ticker).
    - Fetches the window (daily or intraday segments).
    - Calls compute_*_metrics with that baseline.
    """
    # ➕ 1. Build baseline average daily volume over past lookback_days
    lb_start = (start - timedelta(days=lookback_days)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    lb_end   = (start - timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    df_lb = batch_downloader(tickers, "daily", lb_start, lb_end, auth_token)
    baseline_avg_daily = (
        df_lb.groupby("Ticker")["Volume"].mean().to_dict()
    )

    # 2. Fetch the main window
    if mode == "daily":
        df_win = batch_downloader(tickers, mode, start, end, auth_token)
    else:
        # ➕ inline your existing 3-segment split logic
        segments = []
        first_end = datetime.combine(start.date(), MARKET_CLOSE)
        segments.append(("intra", start, first_end))

        next_day  = start.date() + timedelta(days=1)
        last_full = end.date() if end.time()==MARKET_CLOSE else end.date() - timedelta(days=1)
        if next_day <= last_full:
            seg_start = datetime.combine(next_day, MARKET_CLOSE)
            seg_end   = datetime.combine(last_full, MARKET_CLOSE)
            segments.append(("daily", seg_start, seg_end))

        if end.time() != MARKET_CLOSE:
            last_start = datetime.combine(end.date(), MARKET_OPEN)
            segments.append(("intra", last_start, end))

        dfs = [ batch_downloader(
                    tickers,
                    "daily" if m=="daily" else "intra",
                    s, e, auth_token)
                for m,s,e in segments ]
        df_win = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if df_win.empty:
        return pd.DataFrame()

    # 3. Compute and return metrics with baseline
    if mode == "daily":
        return compute_daily_metrics(df_win, baseline_avg_daily)
    else:
        return compute_intraday_metrics(df_win, baseline_avg_daily)
    
# def run_screener(tickers, mode, start, end, auth_token):
#     """
#     Run the stock screener on the given tickers.
#     start and end are datetime objects.
#     mode can be "daily" or "intra".
#     This function returns a DataFrame with the screener results.
#     """
#     if mode == "daily":
#         df = batch_downloader(tickers, mode, start, end, auth_token)
#         print(df)
#         return compute_daily_metrics(df) if not df.empty else df

#     # intraday: split into first partial day, full days, last partial day
#     segments = []
#     first_end = datetime.combine(start.date(), MARKET_CLOSE)
#     segments.append(("intra", start, first_end))

#     next_day = start.date() + timedelta(days=1)
#     last_full = end.date() if end.time()==MARKET_CLOSE else end.date()-timedelta(days=1)
#     if next_day <= last_full:
#         seg_start = datetime.combine(next_day, MARKET_CLOSE)
#         seg_end   = datetime.combine(last_full, MARKET_CLOSE)
#         segments.append(("daily", seg_start, seg_end))

#     if end.time() != MARKET_CLOSE:
#         last_start = datetime.combine(end.date(), MARKET_OPEN)
#         segments.append(("intra", last_start, end))

#     dfs = []
#     for seg_mode, s, e in segments:
#         dfs.append(batch_downloader(tickers,
#                                    "daily" if seg_mode=="daily" else "intra",
#                                    s, e, auth_token))
#     df_all = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
#     return compute_intraday_metrics(df_all) if not df_all.empty else df_all