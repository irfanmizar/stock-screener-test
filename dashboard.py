import streamlit as st
import pandas as pd
import yfinance as yf
from screener import run_screener
from datetime import date, timedelta, time

# date limits
st.title("Stock Screener Prototype")
col1, col2, col3, col4, col5, col6 = st.columns(6)
today = date.today()

# Start date
with col1:
    start_date = st.date_input(
        "Start Date", 
        value=today,
        max_value=today
    )

# End date
with col2:
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

# Start time (hour selection)
with col3:
    if time_disable:
        hour_options = [OPEN_HOUR]  # default to market open if time is disabled
    else:
        hour_options = list(range(OPEN_HOUR, CLOSE_HOUR + 1))
    start_hour = st.selectbox("Start Time", hour_options, index=0)

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

with col4:
    start_minute = st.selectbox(":", start_minute_options, index=0)

# End time (hour selection)
with col5:
    if time_disable:
        hour_options = [CLOSE_HOUR]  # default to market close if time is disabled
    else:
        hour_options = list(range(OPEN_HOUR, CLOSE_HOUR + 1))
    end_hour = st.selectbox("End Time", hour_options, index=len(hour_options) - 1)

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

with col6:
    end_minute = st.selectbox(":", end_minute_options, index=len(end_minute_options) - 1)

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

# Convert start and end dates to datetime
start = pd.to_datetime(f"{start_date} {start_time}")
end = pd.to_datetime(f"{end_date} {end_time}")

num_days = (end - start).days

if num_days <= 7:
    interval = "1m"
elif num_days <= 60:
    interval = "2m"
else:
    interval = "1d"

# tickers_df = pd.read_csv("stock_tickers.csv")
# st.subheader("Available Tickers")
# st.dataframe(tickers_df)
# tickers = ["MMM", "AAPL", "MSFT", "GOOGL", "NVDA", "AMD", "CFSB"]
# tickers_df = pd.DataFrame({"Ticker": tickers})
# st.subheader("Available Tickers")
# st.dataframe(tickers_df)
tickers_csv = st.file_uploader("Upload a CSV file with stock tickers", type=["csv"])
if tickers_csv is not None:
    tickers_df = pd.read_csv(tickers_csv)
    if "Ticker" not in tickers_df.columns:
        st.error("CSV must contain a 'Ticker' column.")
        st.stop()
    tickers = tickers_df["Ticker"].astype(str).tolist()

# print(tickers_df.columns)
if st.button("Run Screener"):
    df = run_screener(
        tickers=tickers,
        interval=interval,
        start=start,
        end=end,
    )
    if df.empty:
        st.write("No stocks passed the screener.")
    else:
        st.subheader("Screener Results")
        st.dataframe(df)