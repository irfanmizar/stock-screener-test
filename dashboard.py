import streamlit as st
import pandas as pd
import yfinance as yf
from screener import run_screener
from datetime import date, timedelta

# date limits
today = date.today()
st.title("Stock Screener Prototype")

# period = st.selectbox("Period", ["1d", "2d", "5d"], index=0)
start = st.date_input(
    "Start Date", 
    value=today - timedelta(days=7),
    min_value=today - timedelta(days=60),
    max_value=today
)

end = st.date_input(
    "End Date", 
    value=today,
    min_value=start,
    max_value=today
)

lookback = (end - start).days

INTERVALS = {
    "1m": 1,
    "5m": 1,
    "15m": 1,
    "1d": 55,
}

valid_intervals = []
for iv, max_days in INTERVALS.items():
    if max_days is None or lookback <= max_days:
        valid_intervals.append(iv)

if not valid_intervals:
    st.error("The selected date range is too long for the available intervals.")
    st.stop()


interval = st.selectbox("Interval", valid_intervals, index=valid_intervals.index("1d") if "1d" in valid_intervals else 0)
prepost = st.checkbox("Include Pre/Post Market Data", value=True)


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
        prepost,
        start=start,
        end=end,
    )
    if df.empty:
        st.write("No stocks passed the screener.")
    else:
        st.subheader("Screener Results")
        st.dataframe(df)