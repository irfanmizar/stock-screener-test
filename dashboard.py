import streamlit as st
import pandas as pd
import yfinance as yf
from filters.momentum import momentum_screener
from screener import run_screener

st.title("Stock Screener Prototype")

period = st.selectbox("Period", ["1d", "2d", "5d"], index=0)
interval = st.selectbox("Interval", ["1m", "5m", "15m"], index=0)
prepost = st.checkbox("Include Pre/Post Market Data", value=True)
momentum_threshold = st.number_input("Momentum Threshold (%)", value=0.75, min_value=0.0, max_value=100.0)
volume_threshold = st.number_input("Volume Spike Threshold (%)", value=90.0, min_value=0.0, max_value=100.0)

tickers_df = pd.read_csv("stock_tickers.csv")
st.subheader("Available Tickers")
st.dataframe(tickers_df)

if st.button("Run Screener"):
    df = run_screener(
        tickers_df["Ticker"].tolist(),
        period,
        interval,
        prepost,
        momentum_threshold,
        volume_threshold
    )
    if df.empty:
        st.write("No stocks passed the screener.")
    else:
        st.subheader("Screener Results")
        st.dataframe(df)