import streamlit as st
import pandas as pd
import yfinance as yf
from screener import run_screener
from datetime import date, timedelta, time
from millify import millify as mf
import math

# date limits
st.title("Stock Screener Prototype")
today = date.today()
prepost = False

with st.sidebar:
    start_date = st.date_input(
        "Start Date", 
        value=today,
        max_value=today
    )

    end_date = st.date_input(
        "End Date",
        value=today,
        min_value=start_date,
        max_value=today
    )

    date_interval = (today - start_date).days

    OPEN_HOUR = 9
    OPEN_MINUTE = 30
    CLOSE_HOUR = 16
    CLOSE_MINUTE = 0

    if date_interval > 60:
        time_disable = True
    else:
        time_disable = False

    col1, col2 = st.columns(2)
    # Start time (hour selection)
    with col1:
        if time_disable:
            hour_options = [CLOSE_HOUR]  # default to market open if time is disabled
        else:
            hour_options = list(range(OPEN_HOUR, CLOSE_HOUR + 1))
        start_hour = st.selectbox("Start Hour", hour_options, index=0)

    # Start time (minute selection)
    if (date_interval <= 7):
        if (start_hour == 9):
            start_minute_options = list(range(OPEN_MINUTE, 60))
        else:
            start_minute_options = list(range(0, 60))
    elif (date_interval <= 60):
        if (start_hour == 9):
            start_minute_options = list(range(OPEN_MINUTE, 60, 2))
        else:
            start_minute_options = list(range(0, 60, 2))
    else:
        start_minute_options = CLOSE_MINUTE

    with col2:
        start_minute = st.selectbox("Start Minute", start_minute_options, index=0)

    # End time (hour selection)
    with col1:
        if time_disable:
            hour_options = [CLOSE_HOUR]  # default to market close if time is disabled
        else:
            hour_options = list(range(OPEN_HOUR, CLOSE_HOUR + 1))
        end_hour = st.selectbox("End Hour", hour_options, index=len(hour_options) - 1)

    # End time (minute selection)
    if (date_interval <= 7):
        if (end_hour == 16):
            end_minute_options = list(range(0, CLOSE_MINUTE + 1))
        else:
            end_minute_options = list(range(0, 60))
    elif (date_interval <= 60):
        if (end_hour == 16):
            end_minute_options = list(range(0, CLOSE_MINUTE + 1, 2))
        else:
            end_minute_options = list(range(0, 60, 2))
    else:
        end_minute_options = [CLOSE_MINUTE]

    with col2:
        end_minute = st.selectbox("End Minute", end_minute_options, index=len(end_minute_options) - 1)

start_time = time(start_hour, start_minute)
end_time = time(end_hour, end_minute)
market_open = time(9,30)
market_close = time(16,0)

if not (market_open <= start_time <= market_close):
    st.error("Start Time must be between 09:30 and 16:00")
    st.stop()

if not (market_open <= end_time <= market_close):
    st.error("End Time must be between 09:30 and 16:00")
    st.stop()

if start_date == end_date and end_time < start_time:
    st.error("End Time cannot be earlier than Start Time on the same day")
    st.stop()

if end_hour > 15 and end_minute >= 0:
    prepost = True

# overwrite the above
end_time = time(end_hour, end_minute)

# Convert start and end dates to datetime
start = pd.to_datetime(f"{start_date} {start_time}")
end = pd.to_datetime(f"{end_date} {end_time}")
# print(start, end)

num_days = (end - start).days

if num_days <= 7:
    interval = "1m"
elif num_days <= 60:
    interval = "2m"
else:
    interval = "1d"

tickers_csv = st.file_uploader("Upload a CSV file with stock tickers", type=["csv"])
if tickers_csv is not None:
    tickers_df = pd.read_csv(tickers_csv)
    if "Ticker" not in tickers_df.columns:
        st.error("CSV must contain a 'Ticker' column.")
        st.stop()
    tickers = tickers_df["Ticker"].astype(str).tolist()

 #print("Prepost: ", prepost)
if st.button("Run Screener"):
    df = run_screener(
        tickers=tickers,
        interval=interval,
        start=start,
        end=end,
        num_days=num_days,
        prepost=True
    )
    if df.empty:
        st.write("No stocks passed the screener.")

    st.session_state["raw"] = df
    st.session_state["filtered"] = df.copy()
    st.session_state["show_results"] = True


if st.session_state.get("show_results") and "raw" in st.session_state:
    raw = st.session_state["raw"]

    st.subheader("Screener Results")
    if raw.empty:
        st.write("No stocks passed the screener.")
    else:
        # Detect schema (daily vs intraday) and set labels
        is_daily = "Total Volume" in raw.columns
        vol_col  = "Total Volume"   if is_daily else "Min Total Vol"
        avg_col  = "Average Volume" if is_daily else "Avg Vol/Min"
        rvol_col = "Relative Volume" if is_daily else "RVol (min)"
        rank_choices = (["PC (%)", rvol_col, avg_col, vol_col]
                        if is_daily else ["PC (%)","RVol (day)","RVol (min)","Avg Vol/Min","Min Total Vol"])

        with st.expander("Filters", expanded=True):
            with st.form("filter_form"):
                c1, c2, c3 = st.columns(3)
                pc_rng  = c1.slider("PC% range", -20.0, 20.0, (-2.0, 2.0))
                min_vol = c2.number_input(f"Min {vol_col}", value=(1_000_000 if is_daily else 500_000), step=50_000)
                min_avg = c3.number_input(f"Min {avg_col}", value=(1_000_000 if is_daily else 2_000), step=100)
                min_rv  = c1.number_input(f"Min {rvol_col}", value=0.8, step=0.1)
                if not is_daily:
                    min_rv_day = c2.number_input("Min RVol (day)", value=0.8, step=0.1)
                metric = c3.selectbox("Rank by", rank_choices, index=0)
                top_n  = c3.number_input("Show top N", value=50, step=10)
                apply  = st.form_submit_button("Apply filters")

        # apply filters on click, then keep them in session_state
        f = raw.copy()
        if apply:
            f = f[
                (f["PC (%)"].between(pc_rng[0], pc_rng[1])) &
                (f[vol_col] >= min_vol) &
                (f[avg_col] >= min_avg) &
                (f[rvol_col] >= min_rv)
            ]
            if not is_daily:
                f = f[f["RVol (day)"] >= min_rv_day]
            f = f.sort_values(metric, ascending=False).head(top_n)
            st.session_state["filtered"] = f

        st.dataframe(st.session_state.get("filtered", raw), use_container_width=True)

        colA, colB = st.columns(2)
        with colA:
            if st.button("Reset filters"):
                st.session_state["filtered"] = raw.copy()
        with colB:
            if st.button("Clear results"):
                for k in ("raw","filtered","show_results"):
                    st.session_state.pop(k, None)
                st.rerun()

# st.dataframe(styled_df, use_container_width=True)


    # else:
    #     def color_change(val):
    #         if isinstance(val, (int, float)):
    #             if val > 0:
    #                 color = 'green'
    #             elif val < 0:
    #                 color = 'red'
    #             else:
    #                 color = 'grey'
    #         else:
    #             color = 'grey'
    #         return f'color: {color}'
        
    #     def fmt_pct(x):
    #         return f"{x:.2f}%"
        
    #     def fmt_mill(x):
    #         if x is None or (isinstance(x, float) and math.isnan(x)):
    #             return ""
    #         return mf(x, precision=2)
        
    #     styled_df = (df.style.format({
    #         "Price": "{:,.2f}",
    #         "PC (%)": fmt_pct,
    #         "Min Total Vol": fmt_mill,
    #         "Avg Vol/Min": fmt_mill,
    #         "RVol (min)": fmt_mill,
    #         "Day Total Vol": fmt_mill,
    #         "Avg Vol/Day": fmt_mill,
    #         "RVol (day)": fmt_mill,
    #         "Total Volume": fmt_mill,
    #         "Average Volume": fmt_mill,
    #         "Relative Volume": fmt_mill
    #     }).map(color_change, subset=["PC (%)"]))