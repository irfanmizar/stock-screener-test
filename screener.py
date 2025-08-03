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
    return ser.loc[(ser.index >= start_dt) & (ser.index <= end_dt)]

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
    first_open = df_slice["Close"].iloc[0]
    last_close = df_slice["Close"].iloc[-1]
    pct_change = ((last_close - first_open) / first_open * 100) if first_open else 0

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
        # per_min_base = baseline_daily / 390
        # rel_vol       = avg_vol / per_min_base if per_min_base else 0
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

    print(
        f"[compute_metrics] is_intraday={is_intraday}\n"
        f"  first_open={first_open:.2f}, last_close={last_close:.2f}, pct_change={pct_change:.2f}%\n"
        f"  raw_volumes_count={len(df_slice)}, volumes_sum={df_slice['Volume'].sum()}\n"
        f"  used_volumes_sum={total_vol}, avg_vol={avg_vol:.2f}\n"
        f"  baseline_daily={baseline_daily:.2f}\n"
        f"  rel_vol={rel_vol:.2f}"
    )
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
            # if num_days > 7:
            #    df_min = pd.DataFrame()
            if hasattr(df_min.index, "tz") and df_min.index.tz is not None:
                 df_min.index = df_min.index.tz_convert(None)
            # restrict to market hours
            # df_min = df_min.between_time(time(9,30), time(16,0))
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
            
            if start.time() != time(16, 0) and len(df_day) > 2:
                df_day = df_day[df_day.index.date > start.date()]
            if end.time() != time(16,0):
                df_day = df_day[df_day.index.date < end.date()]
            print(f"[run_screener] raw df_day dates for {batch}: {df_day.index.date.tolist()}")
            # if start.time() != time(9,30) and len(df_day) > 2:
            #     df_day = df_day[df_day.index.date > start.date()]
            if hasattr(df_day.index, "tz") and df_day.index.tz is not None:
                 df_day.index = df_day.index.tz_convert(None)
            
            # now loop each ticker once
            for ticker in batch:
                sym = ticker.replace("-", ".")
                
                # minute‐bars for this ticker
                 #if sym in df_min.columns:
                if sym in df_min.columns and not df_min.empty:
                    mins = df_min[sym]["Volume"].fillna(0)
                    total_min = int(mins.sum())
                    avg_min   = float(mins.mean())
                else:
                    total_min = avg_min = 0.0
                
                # daily bars for this ticker
                if sym in df_day.columns:
                    # raw_days   = df_day[sym]["Volume"].fillna(0)
                    # day_slice  = raw_days.iloc[1:]           # exclude first bar
                    # total_day  = int(day_slice.sum())
                    # avg_day    = float(day_slice.mean()) if len(day_slice)>0 else 0.0
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
                # print(baseline_daily_avg)
                rvol_min = avg_min / (baseline_daily_avg/390) if baseline_daily_avg else 0
                rvol_day = avg_day / baseline_daily_avg        if baseline_daily_avg else 0
                # print("RVOL: ", rvol_day)
                
                # price change over the full window
                # you could pull last Close / first Open from either df_min or df_day
                # here we’ll pick df_day if available, else df_min
                if sym in df_day.columns:
                    first_o = df_day[sym]["Open"].iloc[0]
                    last_c  = df_day[sym]["Close"].iloc[-1]
                elif sym in df_min.columns:
                    first_o = df_min[sym]["Open"].iloc[0]
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

        # # — Intraday mix otherwise
        # else:
        #     # A) first partial day
        #     first_end = datetime.combine(start.date(), time(16,0))
        #     try:
        #         df_first  = yf.download(
        #             tickers=batch, start=start, end=first_end,
        #             interval="1m", group_by="ticker",
        #             auto_adjust=False, threads=True, progress=False,
        #             prepost=prepost
        #         )
        #     except Exception as e:
        #         print(f"No intraday 1m data for {batch} between {start} and {first_end}")
        #         df_first = pd.DataFrame()
            
        #     if hasattr(df_first.index, "tz") and df_first.index.tz is not None:
        #         df_first.index = df_first.index.tz_convert(None)

        #     # B) full days in the middle
        #     next_day  = start.date() + timedelta(days=1)
        #     last_full = end.date() if end.time()==time(16,0) else end.date()-timedelta(days=1)
        #     if next_day <= last_full:
        #         try:
        #             df_mid = yf.download(
        #                 tickers=batch,
        #                 start=next_day,
        #                 end=(last_full+timedelta(days=1)),
        #                 interval="1d",
        #                 group_by="ticker",
        #                 auto_adjust=False,
        #                 threads=True,
        #                 progress=False,
        #                 prepost=False
        #             )
        #         except Exception as e:
        #             print(f"No daily data for {batch} between {next_day} and {last_full}")
        #             df_mid = pd.DataFrame()
        #         if hasattr(df_mid.index, "tz") and df_mid.index.tz is not None:
        #             df_mid.index = df_mid.index.tz_convert(None)
        #     else:
        #         df_mid = pd.DataFrame()

        #     # C) last partial day
        #     if end.time() != time(16,0):
        #         last_start = datetime.combine(end.date(), time(9,30))
        #         try:
        #             df_last    = yf.download(
        #                 tickers=batch,
        #                 start=last_start,
        #                 end=end,
        #                 interval="1m",
        #                 group_by="ticker",
        #                 auto_adjust=False,
        #                 threads=True,
        #                 progress=False,
        #                 prepost=prepost
        #             )
        #         except Exception as e:
        #             print(f"No intraday 1m data for {batch} between {last_start} and {end}")
        #             df_last = pd.DataFrame()

        #         if hasattr(df_last.index, "tz") and df_last.index.tz is not None:
        #             df_last.index = df_last.index.tz_convert(None)
        #     else:
        #         df_last = pd.DataFrame()

        #     # Loop each ticker
        #     for ticker in batch:
        #         sym = ticker.replace("-",".")
        #         # slice
        #         s1 = slice_window(df_first, sym,   start,  first_end)
        #         s2 = df_mid[sym] if (not df_mid.empty and sym in df_mid.columns) else pd.DataFrame()
        #         s3 = slice_window(df_last,  sym,
        #                           datetime.combine(end.date(), time(9,30)),
        #                           end)
                
        #         df_slice = pd.concat([s1,s2,s3]).sort_index()
        #         if df_slice.empty:
        #             continue

        #         # ── Split into minute‐bars and full‐day segments
        #         mins = pd.concat([s1, s3]).sort_index()     # only partial‐day minute bars
        #         days = s2                                   # full 1d bars in the middle

        #        # ➕ MINUTE‐BAR METRICS
        #         min_vols = mins["Volume"].fillna(0)
        #         total_min = int(min_vols.sum())
        #         avg_min   = float(min_vols.mean()) if len(min_vols)>0 else 0
        #         # baseline daily avg for RVol calculation
        #         baseline_daily_avg = (
        #             df_baseline[sym]["Volume"].mean()
        #             if sym in df_baseline.columns else avg_min
        #         )
        #         # rvol_min = avg_min / (baseline_daily_avg/390) if baseline_daily_avg else 0
        #         if len(mins.index) >= 2:
        #             bar_delta = (mins.index[1] - mins.index[0]).total_seconds() / 60
        #         else:
        #             bar_delta = 1.0    # fallback to 1 minute if only one bar

        #         bars_per_day = 390.0 / bar_delta
        #         baseline_per_bar = baseline_daily_avg / bars_per_day if bars_per_day else baseline_daily_avg
        #         rvol_min = avg_min / baseline_per_bar if baseline_per_bar else 0

        #         # ➕ DAILY‐BAR METRICS
        #         day_vols = days["Volume"].fillna(0)
        #         if len(day_vols)>1:
        #             first_day_vol = day_vols.iloc[0]
        #             total_day     = int(day_vols.sum() - first_day_vol)
        #             avg_day       = total_day / (len(day_vols)-1)
        #         else:
        #             total_day     = int(day_vols.sum())
        #             avg_day       = float(total_day)
        #         rvol_day = avg_day / baseline_daily_avg if baseline_daily_avg else 0

        #         pct_change = (
        #             (df_slice["Close"].iloc[-1] - df_slice["Open"].iloc[0])
        #             / df_slice["Open"].iloc[0] * 100
        #             if not df_slice.empty else 0
        #         )

        #         # ➕ Append both metric sets
        #         passed.append({
        #             "Ticker":         sym,
        #             "Price":          round(df_slice["Close"].iloc[-1],2),
        #             "PC (%)":         round(pct_change,2),
        #             # minute‐bar metrics
        #             "Min Total Vol":  total_min,
        #             "Avg Vol/Min":    avg_min,
        #             "RVol (min)":     round(rvol_min,2),
        #             # daily‐bar metrics
        #             "Day Total Vol":  total_day,
        #             "Avg Vol/Day":    avg_day,
        #             "RVol (day)":     round(rvol_day,2)
        #         })

# def stock_data(df, df_daily, df_lookback, ticker, company_name, passed, start, end, interval):
#     """
#     Fetch the corresponding data for the ticker in the given period.
#     """
#     # print(df.tail())
#     price_change = price_change_percentage(df_daily)
#     price_change_abs = price_change_absolute(df_daily)
#     current_volume = volume(df, df_daily)
#     daily_volume = average_volume_daily(df_daily)
#     avg_volume = average_volume(df_lookback)
#     rel_volume = relative_volume(avg_volume, daily_volume)
    
#     # info = yf.Ticker(ticker).info
#     passed.append({
#         "Ticker": ticker,
#         # "Name": company_name,
#         "Price": round(df_daily["Close"].iloc[-1], 2),
#         "PC (%)": round(price_change, 2),
#         "PC ": round(price_change_abs, 2),
#         "PC2 (%)": round(price_change_percentage_opentoclose(df), 2),
#         "Total Volume": int(current_volume),
#         "Average Daily Volume": int(daily_volume),
#         "Average Volume": int(avg_volume),
#         "Relative Volume ": round(rel_volume, 2)
#     })

# def run_screener(tickers, interval, start, end, num_days, prepost):
#     """
#     Same signature as before.
#     Splits into 3 scenarios:
#       • daily-only if start/end at 16:00
#       • otherwise: first intraday partial, full-day daily, last intraday partial
#     """
#     passed = []

#     # 1) Build 90-day lookback for baseline daily volume
#     lb_start = (start - pd.Timedelta(days=90)).date()
#     lb_end   = (start - pd.Timedelta(days=1)).date()
#     df_lookback = yf.download(
#         tickers,
#         start=lb_start,
#         end=(lb_end + timedelta(days=1)),
#         interval="1d",
#         group_by="ticker",
#         auto_adjust=False,
#         threads=True,
#         progress=False,
#         prepost=False
#     )
#     # df_lookback.index = df_lookback.index.tz_convert(None)

#     # 2) Batch up to 200 tickers at a time
#     for i in range(0, len(tickers), 200):
#         batch = tickers[i:i+200]

#         # ── Daily-only path ───────────────────────────────────────────────────────
#         if start.time() == time(16,0) and end.time() == time(16,0):
#             # grab daily bars for [start.date() … end.date()]
#             df_daily = yf.download(
#                 tickers=batch,
#                 start=start.date(),
#                 end=(end.date() + timedelta(days=1)),
#                 interval="1d",
#                 group_by="ticker",
#                 auto_adjust=False,
#                 threads=True,
#                 progress=False,
#                 prepost=False
#             )
#             # df_daily.index = df_daily.index.tz_convert(None)

#             # feed through your old loop
#             for ticker in batch:
#                 symbol = ticker.replace("-", ".")
#                 company_name = ticker  # adjust if you had a name field

#                 # daily slice is simply the daily DataFrame
#                 df_slice = df_daily[symbol] if symbol in df_daily.columns else pd.DataFrame()
#                 if df_slice.empty:
#                     continue

#                 df_bl = df_lookback[symbol] if symbol in df_lookback.columns else pd.DataFrame()

#                 # compute exactly as before
#                 stock_data(df_slice, df_slice, df_bl, symbol, company_name,
#                            passed, start, end, "1d")

#         # ── Intraday path ────────────────────────────────────────────────────────
#         else:
#             # A) first partial day: [start → today@16:00]
#             first_end = datetime.combine(start.date(), time(16,0))
#             df_first = yf.download(
#                 tickers=batch,
#                 start=start,
#                 end=first_end,
#                 interval="1m",
#                 group_by="ticker",
#                 auto_adjust=False,
#                 threads=True,
#                 progress=False,
#                 prepost=prepost
#             )
#             # df_first.index = df_first.index.tz_convert(None)

#             # B) full days (if any): [next_day@16:00 → last_full_day@16:00]
#             next_day  = start.date() + timedelta(days=1)
#             last_full = end.date() if end.time()==time(16,0) else end.date()-timedelta(days=1)
#             if next_day <= last_full:
#                 df_mid = yf.download(
#                     tickers=batch,
#                     start=next_day,
#                     end=(last_full + timedelta(days=1)),
#                     interval="1d",
#                     group_by="ticker",
#                     auto_adjust=False,
#                     threads=True,
#                     progress=False,
#                     prepost=False
#                 )
#                 # df_mid.index = df_mid.index.tz_convert(None)
#             else:
#                 df_mid = pd.DataFrame()

#             # C) last partial day (if end not 16:00): [today@09:30 → end]
#             if end.time() != time(16,0):
#                 last_start = datetime.combine(end.date(), time(9,30))
#                 df_last = yf.download(
#                     tickers=batch,
#                     start=last_start,
#                     end=end,
#                     interval="1m",
#                     group_by="ticker",
#                     auto_adjust=False,
#                     threads=True,
#                     progress=False,
#                     prepost=prepost
#                 )
#                 # df_last.index = df_last.index.tz_convert(None)
#             else:
#                 df_last = pd.DataFrame()

#             # Now loop tickers & assemble slices exactly as before
#             for ticker in batch:
#                 symbol = ticker.replace("-", ".")
#                 company_name = ticker

#                 # slice each segment
#                 slice1 = slice_window(df_first, symbol, start, first_end)
#                 slice2 = (
#                     df_mid[symbol]
#                     if not df_mid.empty and symbol in df_mid.columns
#                     else pd.DataFrame()
#                 )
#                 slice3 = slice_window(df_last, symbol,
#                                       datetime.combine(end.date(), time(9,30)), end)

#                 # concat into one continuous DataFrame
#                 df_slice = pd.concat(
#                     [slice1, slice2, slice3]
#                 ).sort_index()

#                 if df_slice.empty:
#                     continue

#                 # baseline daily for this ticker
#                 df_bl = df_lookback[symbol] if symbol in df_lookback.columns else pd.DataFrame()

#                 # now compute using your existing helper
#                 p_change, p_change_abs, total_vol, avg_daily_vol, rel_vol = compute_metrics(
#                     df_slice, df_bl, None
#                 )

#                 passed.append({
#                     "Ticker": symbol,
#                     "Name":   company_name,
#                     "Price":  round(df_slice["Close"].iloc[-1], 2),
#                     "PC (%)": round(p_change, 2),
#                     "PC ":    round(p_change_abs, 2),
#                     "PC2 (%)": round(price_change_percentage_opentoclose(df_slice), 2),
#                     "Total Volume":            int(total_vol),
#                     "Average Daily Volume":    int(avg_daily_vol),
#                     "Average Volume":          int(avg_daily_vol),
#                     "Relative Volume ":        round(rel_vol, 2)
#                 })

#     return pd.DataFrame(passed)
# def run_screener(tickers, interval, start, end, num_days, prepost):
#     """
#     Run the stock screener on the given tickers.
#     """
#     passed = []
#     for i in range(0, len(tickers), 200):
#         batch = tickers[i:i+200]

#         df_batch = yf.download(
#         tickers=batch,
#         start=start,
#         end=end,
#         interval=interval,
#         group_by="ticker",
#         auto_adjust=False,
#         threads=True,
#         progress=False,
#         prepost=prepost
#         )

#         df_batch.index = df_batch.index.tz_convert(None)

#         df_daily = yf.download(
#             tickers=batch,
#             start=start,
#             end=end,
#             interval="1d",
#             group_by='ticker',
#             auto_adjust=False,
#             threads= True,
#             progress=False,
#             prepost=False
#         )

#         df_lookback = yf.download(
#             tickers=batch,
#             start=(start - pd.Timedelta(days=90)).strftime("%Y-%m-%d"),
#             end=start.strftime("%Y-%m-%d"),
#             interval="1d",
#             group_by='ticker',
#             auto_adjust=False,
#             threads= True,
#             progress=False,  # Disable progress bar for cleaner output
#             prepost=False  # Disable pre/post market data for cleaner output
#         )

#         for ticker in batch:
#             symbol = ticker.replace("-", ".")
#             company_name = ticker['Company'] if isinstance(ticker, dict) and 'Company' in ticker else ticker

#             print(f"Processing ticker: {symbol} ({company_name})")
#             df_slice = slice_window(df_batch, symbol, start, end)
#             if df_slice.empty:
#                 print(f"No data for ticker {symbol} in the specified date range.")
#                 continue

#             df_3m_daily = df_daily[symbol] if symbol in df_daily.columns else pd.DataFrame()
#             df_3m_intraday = df_batch[symbol] if symbol in df_batch.columns else pd.DataFrame()

#             p_change, p_change_abs, total_volume, avg_daily_volume, rel_vol_eod = compute_metrics(df_slice, df_3m_daily, None)

#             passed.append({
#                 "Ticker": symbol,
#                 "Name": company_name,
#                 "Price": round(df_slice["Close"].iloc[-1], 2),
#                 "PC (%)": round(p_change, 2),
#                 "PC ": round(p_change_abs, 2),
#                 "PC2 (%)": round(price_change_percentage_opentoclose(df_slice), 2),
#                 "Total Volume": int(total_volume),
#                 "Average Daily Volume": int(avg_daily_volume),
#                 "Average Volume": int(avg_daily_volume) if not df_lookback.empty else 0,
#                 "Relative Volume ": round(rel_vol_eod, 2)
#             })


#         # for sym in batch:
#         #     symbol = sym.replace("-", ".")
#         #     company_name = sym['Company'] if isinstance(sym, dict) and 'Company' in sym else sym
#         #     # Check if the ticker exists in the downloaded data
#         #     if sym not in df_batch.columns:
#         #         print(f"Ticker {sym} has no data.")
#         #         continue
#         #     try:
#         #         df_sym = df_batch[sym].dropna(subset=["Close"])
#         #         df_dsym = df_daily[sym]
#         #         df_lookback_sym = df_lookback[sym]
#         #     except KeyError:
#         #         print(f"Ticker {sym} has no data.")
#         #         continue
#         #     if df_sym.empty or df_dsym.empty or df_lookback_sym.empty:
#         #         print(f"Ticker {sym} has no data.")
#         #         continue
#         #     symbol = sym.replace("-", ".")
#         #     print(sym)
#         #     # print(df_batch[sym].head(10))
#         #     # print(df_batch[sym].tail(5))

#         #     stock_data(df_sym, df_dsym, df_lookback_sym, symbol, company_name, passed, start, end, interval)
    
#     return pd.DataFrame(passed)

# def price_change_percentage(df_daily):
#     """
#     Calculate the percentage change in price from the first to the last day."""
#     if df_daily.empty:
#         return 0
#     start_price = df_daily["Close"].iloc[0]
#     end_price = df_daily["Close"].iloc[-1]
#     return ((end_price - start_price) / start_price) * 100

# def price_change_absolute(df_daily):
#     """
#     Calculate the absolute price change from the first to the last day.
#     """
#     if df_daily.empty:
#         return 0
#     start_price = df_daily["Close"].iloc[0]
#     end_price = df_daily["Close"].iloc[-1]
#     if pd.isna(start_price) or pd.isna(end_price):
#         return 0
#     return end_price - start_price

# def price_change_percentage_opentoclose(df):
#     """
#     Calculate the percentage change in price from the first open to the last close.
#     """
#     if df.empty:
#         return 0
#     start_price = df["Open"].iloc[0]
#     end_price = df["Close"].iloc[-1]
#     return ((end_price - start_price) / start_price) * 100

# def volume(df, df_daily, intraday=False):
#     if df_daily.empty:
#         return 0
#      #print(f"Daily Volume: {df_daily['Volume']}")
#     current_volume = df_daily["Volume"].sum() - df_daily["Volume"].iloc[0]  # Subtract the first volume to get the change
#     print("Head: ", df.head(3))
#     print("Tail: ", df.tail(3))
        

#     # print("Test Volume: ", test_vol)
#     print("Current Volume: ", current_volume)
    
#     if pd.isna(current_volume):
#         return 0
#     return current_volume

# def average_volume_daily(df_daily):
#     if df_daily.empty:
#         return 0
#     daily = df_daily["Volume"].sum() - df_daily["Volume"].iloc[0]
#     row_count = df_daily.shape[0] - 1
#     average_daily_volume = daily / row_count if row_count > 0 else 0
#     if pd.isna(average_daily_volume):
#         return 0
#     return average_daily_volume

# def average_volume(df_lookback):
#     """
#     Calculate the average volume between the start and end of the period.
#     """
#     if df_lookback.empty:
#         return 0
#     avg = df_lookback["Volume"].mean()
#     if pd.isna(avg):
#         return 0
#     return avg

# def relative_volume(avg_volume, current_avg_volume):
#     """
#     Calculate the relative volume for the ticker.
#     """
#     return (current_avg_volume/avg_volume) if avg_volume > 0 else 0

# def slice_window(df_intraday, ticker, start_dt, end_dt):
#     """
#     Slice the intraday data for the ticker between start_dt and end_dt.
#     """
#     if df_intraday.empty:
#         return pd.DataFrame()
#     try:
#         new_df = df_intraday[ticker]
#     except KeyError:
#         print(f"Ticker {ticker} not found in the DataFrame.")
#         return pd.DataFrame()
#     mask = (new_df.index >= start_dt) & (new_df.index <= end_dt)
#     df_intraday = new_df.loc[mask]
#     if df_intraday.empty:
#         return pd.DataFrame()
#     return df_intraday

# def compute_metrics(df_slice, df_3m_daily, df_3m_intraday):
#     print(df_slice.head(3))
#     print(df_slice.tail(3))
#     p0 = df_slice["Open"].iloc[0]
#     p1 = df_slice["Close"].iloc[-1]
#     p_change = (p1 - p0) / p0 * 100 if p0 != 0 else 0
#     p_change_abs = p1 - p0
#     if pd.isna(p_change) or pd.isna(p_change_abs):
#         p_change = 0
#         p_change_abs = 0

#     total_volume = df_slice["Volume"].sum() if not df_slice.empty else 0
#     if pd.isna(total_volume):
#         total_volume = 0
#     avg_daily_volume = df_3m_daily["Volume"].mean() if not df_3m_daily.empty else 0
#     rel_vol_eod = total_volume / avg_daily_volume if avg_daily_volume > 0 else 0

#     return p_change, p_change_abs, total_volume, avg_daily_volume, rel_vol_eod