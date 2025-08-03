import streamlit as st
import pandas as pd
from finviz_screener import run_screener
from datetime import date, timedelta, time
from millify import millify as mf
from dotenv import load_dotenv
import os

load_dotenv()
AUTH_TOKEN = os.getenv("FINVIZ_ELITE_AUTH")
if not AUTH_TOKEN:
    raise RuntimeError("Missing FINVIZ_ELITE_AUTH in environment")

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
            hour_options = [OPEN_HOUR]  # default to market open if time is disabled
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
        start_minute_options = OPEN_MINUTE

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
print(start, end)

mode = "daily"
if end_time == time(16, 0) and start_time == time(16, 0):
    mode = "daily"
else:
    mode = "intra"

tickers_csv = st.file_uploader("Upload a CSV file with stock tickers", type=["csv"])
if tickers_csv is not None:
    tickers_df = pd.read_csv(tickers_csv)
    if "Ticker" not in tickers_df.columns:
        st.error("CSV must contain a 'Ticker' column.")
        st.stop()
    tickers = tickers_df["Ticker"].astype(str).tolist()

if st.button("Run Screener"):
    df = run_screener(
        tickers=tickers,
        mode=mode,
        start=start,
        end=end,
        auth_token=AUTH_TOKEN,
    )
    if df.empty:
        st.write("No stocks passed the screener.")
    else:
        def color_change(val):
            if isinstance(val, (int, float)):
                if val > 0:
                    color = 'green'
                elif val < 0:
                    color = 'red'
                else:
                    color = 'grey'
            else:
                color = 'grey'
            return f'color: {color}'
        
        def fmt_pct(x):
            return f"{x:.2f}%"
        
        def fmt_mill(x):
            return mf(x, precision=2)
        
        styled_df = (df.style.format({
            # "Price": "{:,.2f}",
            "Price Change (%)": fmt_pct,
            "Total Volume": fmt_mill,
            "Average Volume": fmt_mill,
        }).map(color_change, subset=["Price Change (%)"]))

        st.subheader("Screener Results")
        st.dataframe(styled_df, use_container_width=True)
