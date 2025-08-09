import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, time
from millify import millify as mf

def slice_window(df_intraday: pd.DataFrame, ticker: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """
    From yf.download(..., group_by='ticker') at 1m, pull only [start_dt → end_dt]
    for a single ticker.
    """
    if df_intraday.empty or ticker not in df_intraday.columns:
        return pd.DataFrame()
    ser = df_intraday[ticker]
    sliced = ser.loc[(ser.index >= start_dt) & (ser.index <= end_dt)]
    # print(f"[slice_window] {ticker} → slicing {start_dt}–{end_dt}:")
    # print("   → returned timestamps:", sliced.index[ [0, -1] ] if not sliced.empty else "EMPTY")
    return sliced

def compute_metrics(df_slice: pd.DataFrame, df_daily_baseline: pd.DataFrame) -> dict:
    """
    Given:
      • df_slice: DataFrame with columns [Open,High,Low,Close,Volume] over any
        contiguous time window (intraday 1m bars, daily bars, or mix).
      • df_daily_baseline: DataFrame of daily bars (past 90d) for relative volume.

    Returns a dict with:
      - pct_change:  (last Close / first Open) %  
      - total_vol:   sum of all bar volumes  
      - avg_vol:     average volume per bar in the slice  
      - rel_vol:     avg_vol / (baseline avg daily ÷ 390) if intraday else avg_vol / baseline avg daily
    """
    if df_slice.empty:
        return {"pct_change":0, "total_vol":0, "avg_vol":0, "rel_vol":0}

    # Price change: first OPEN → last CLOSE
    first_close = df_slice["Close"].iloc[0]
    last_close = df_slice["Close"].iloc[-1]
    pct_change = ((last_close - first_close) / first_close * 100) if first_close else 0

    # ─── VOLUME ───────────────────────────────────────────────────────────────
    volumes = df_slice["Volume"].fillna(0)
    is_intraday = False
    if len(df_slice) > 1:
        delta = df_slice.index.to_series().diff().dropna().min()
        if delta < pd.Timedelta("1D"):
            is_intraday = True

    if is_intraday:
        # ➕ intraday: average across minute bars only
        minute_mask = df_slice.index.time != time(0,0)
        volumes = volumes.loc[minute_mask]
        total_vol = int(volumes.sum())
        avg_vol   = float(volumes.mean()) 
    else:
        # ➕ daily window: subtract the first day’s volume
        if len(volumes) > 1:
            first_day_vol = volumes.iloc[0]
            total_vol     = int(volumes.sum() - first_day_vol)
            avg_vol       = total_vol / (len(volumes) - 1)
        else:
            total_vol = int(volumes.sum())
            avg_vol   = float(total_vol)
    
    # ─── RELATIVE VOLUME ───────────────────────────────
    baseline_daily = df_daily_baseline["Volume"].mean() if not df_daily_baseline.empty else avg_vol

    if is_intraday:
        # compare avg minute‐bar volume to per‐minute baseline
        if len(df_slice.index) >= 2:
            bar_delta = (df_slice.index[1] - df_slice.index[0]).total_seconds() / 60
        else:
            bar_delta = 1.0
        bars_per_day = 390.0 / bar_delta

        # per-bar baseline
        per_bar_baseline = baseline_daily / bars_per_day if bars_per_day else baseline_daily
        rel_vol          = avg_vol / per_bar_baseline if per_bar_baseline else 0
    else:
        # compare avg daily‐bar volume to daily baseline
        rel_vol       = avg_vol / baseline_daily if baseline_daily else 0
        
    return {
        "pct_change": round(pct_change, 2),
        "total_vol":  total_vol,
        "avg_vol":    round(avg_vol),
        "rel_vol":    round(rel_vol, 2)
    }

def run_screener(tickers, interval, start, end, num_days, prepost):
    passed = []

    # 1) Pull 90-day daily baseline for all tickers
    lb_start = (start - pd.Timedelta(days=90)).date()
    lb_end   = (start - pd.Timedelta(days=1)).date()
    df_baseline = yf.download(
        tickers,
        start=lb_start,
        end=(lb_end + timedelta(days=1)),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=False,
        progress=False,
        prepost=False
    )

    # 2) Process in batches of 200
    for i in range(0, len(tickers), 200):
        batch = tickers[i:i+200]

        # — Daily-only if both times are market close
        if start.time()==time(16,0) and end.time()==time(16,0):
            df_daily = yf.download(
                tickers=batch,
                start=start.date(),
                end=(end.date()+timedelta(days=1)),
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=False,
                progress=False,
                prepost=False
            )

            for ticker in batch:
                print(ticker)
                sym = ticker.replace("-",".")
                # slice the daily DF
                df_slice = df_daily[sym] if sym in df_daily.columns else pd.DataFrame()
                df_bl    = df_baseline[sym] if sym in df_baseline.columns else pd.DataFrame()
                metrics  = compute_metrics(df_slice, df_bl)

                passed.append({
                    "Ticker":            sym,
                    "Price":             round(df_slice["Close"].iloc[-1],2) if not df_slice.empty else None,
                    "PC (%)":            metrics["pct_change"],
                    "Total Volume":      metrics["total_vol"],
                    "Average Volume":    metrics["avg_vol"],
                    "Relative Volume":   metrics["rel_vol"]
                })

        # — Intraday mix otherwise
        else:
            # >>> ADD unified minute‐bar fetch for this batch
            df_min = yf.download(
                tickers=batch,
                start=start,
                end=end,
                interval=interval,          # "1m" or "2m"
                group_by="ticker",
                auto_adjust=False,
                threads=False,
                progress=False,
                prepost=prepost
            )

            if hasattr(df_min.index, "tz") and df_min.index.tz is not None:
                 df_min.index = df_min.index.tz_convert(None)
            # restrict to market hours
            if not df_min.empty and isinstance(df_min.index, pd.DatetimeIndex):
                # make sure it's datetime
                df_min.index = pd.to_datetime(df_min.index)
                df_min = df_min.between_time(time(9,30), time(16,0))
            
            
            # >>> ADD daily‐bar fetch for this batch
            df_day = yf.download(
                tickers=batch,
                start=start.date(),
                end=(end.date() + timedelta(days=1)),
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=False,
                progress=False,
                prepost=False
            )

            
            if start.time() != time(16, 0):
                df_day = df_day[df_day.index.date >= start.date()]
            if end.time() != time(16,0):
                df_day = df_day[df_day.index.date < end.date()]

            # if not df_day.empty:
            #     print(f"[download daily]  start={start.date()}  end={end.date()+timedelta(days=1)}")
            #     print(f"  dates returned: {df_day.index.date.tolist()}  (total rows: {len(df_day)})")
            # else:
            #     print("[download daily] got back an EMPTY dataframe")
            if hasattr(df_day.index, "tz") and df_day.index.tz is not None:
                 df_day.index = df_day.index.tz_convert(None)
            
            # now loop each ticker once
            for ticker in batch:
                sym = ticker.replace("-", ".")
                
                # minute‐bars for this ticker
                if sym in df_min.columns and not df_min.empty:
                    mins = df_min[sym]["Volume"].fillna(0)
                    total_min = int(mins.sum())
                    avg_min   = float(mins.mean())
                else:
                    total_min = avg_min = 0.0
                
                # daily bars for this ticker
                if sym in df_day.columns:
                    days = df_day[sym]["Volume"].fillna(0)
                    if len(days)>1:
                        first_vol = days.iloc[0]
                        total_day = int(days.sum() - first_vol)
                        avg_day   = total_day/(len(days)-1)
                    else:
                        total_day = int(days.sum())
                        avg_day   = float(total_day)
                else:
                    total_day = avg_day = 0.0
                
                # baseline from your 90d df_baseline
                baseline_daily_avg = (
                    df_baseline[sym]["Volume"].mean()
                    if sym in df_baseline.columns else avg_day
                )
                
                # relative vol calculations
                bars_per_day = 390.0 / (2.0 if interval == "2m" else 1.0)
                rvol_min = avg_min / (baseline_daily_avg/bars_per_day) if baseline_daily_avg else 0
                rvol_day = avg_day / baseline_daily_avg        if baseline_daily_avg else 0
                # print("RVOL: ", rvol_day)
                
                # price change over the full window
                if sym in df_day.columns:
                    first_o = df_day[sym]["Close"].iloc[0]
                    last_c  = df_day[sym]["Close"].iloc[-1]
                elif sym in df_min.columns:
                    first_o = df_min[sym]["Close"].iloc[0]
                    last_c  = df_min[sym]["Close"].iloc[-1]
                else:
                    first_o = last_c = None
                pct = ((last_c-first_o)/first_o*100) if first_o else 0
                
                passed.append({
                    "Ticker":        sym,
                    "Price":         round(last_c,2) if last_c else None,
                    "PC (%)":        round(pct,2),
                    "Min Total Vol": total_min,
                    "Avg Vol/Min":   avg_min,
                    "RVol (min)":    round(rvol_min,2),
                    "Day Total Vol": total_day,
                    "Avg Vol/Day":   avg_day,
                    "RVol (day)":    round(rvol_day,2)
                })

    return pd.DataFrame(passed)