import streamlit as st
import pandas as pd
import yfinance as yf
from filters.momentum import momentum_screener
from screener import run_screener

st.title("Stock Screener Prototype")

# period = st.selectbox("Period", ["1d", "2d", "5d"], index=0)
interval = st.selectbox("Interval", ["1m", "5m", "15m", "1d"], index=0)
prepost = st.checkbox("Include Pre/Post Market Data", value=True)
start = st.date_input("Start Date", value=pd.to_datetime("2025-05-01"))
end = st.date_input("End Date", value=pd.to_datetime("2025-05-07"))
# momentum_threshold = st.number_input("Momentum Threshold (%)", value=0.75, min_value=0.0, max_value=100.0)
# volume_threshold = st.number_input("Volume Spike Threshold (%)", value=90.0, min_value=0.0, max_value=100.0)

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