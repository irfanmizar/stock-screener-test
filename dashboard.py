import streamlit as st
import pandas as pd
import yfinance as yf
from screener import run_screener
from datetime import date, timedelta, time

# date limits
today = date.today()
st.title("Stock Screener Prototype")

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

market_open = time(9, 30)
market_close = time(16, 0)

start_time = st.time_input(
    "Start Time",
    value=market_open,
)

end_time = st.time_input(
    "End Time",
    value=market_close,
)

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
elif num_days <= 30:
    interval = "5m"
elif num_days <= 60:
    interval = "15m"
else:
    interval = "1d"

# tickers_df = pd.read_csv("stock_tickers.csv")
# st.subheader("Available Tickers")
# st.dataframe(tickers_df)
tickers = ["MMM", "AAPL", "MSFT", "GOOGL", "NVDA", "AMD", "CFSB"]
tickers_df = pd.DataFrame({"Ticker": tickers})
st.subheader("Available Tickers")
st.dataframe(tickers_df)

if st.button("Run Screener"):
    df = run_screener(
        tickers_df["Ticker"].tolist(),
        interval,
        start=start,
        end=end,
    )
    if df.empty:
        st.write("No stocks passed the screener.")
    else:
        st.subheader("Screener Results")
        st.dataframe(df)