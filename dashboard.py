import streamlit as st
import pandas as pd
import yfinance as yf
from filters.momentum import momentum_screener

st.title("Stock Screener Prototype")

st.sidebar.header("Data")
tickers_df = pd.read_csv("stock_tickers.csv")
tickers      = tickers_df["Ticker"].tolist()

st.sidebar.header("Filters")
mom_thresh = st.sidebar.slider("Momentum threshold (%)", 0.0, 5.0, 0.75)

st.subheader("All tickers")
st.dataframe(tickers_df)

if st.sidebar.button("Run Momentum Screener"):
    passed = []
    for symbol in tickers[:50]:
        yf_sym = symbol.replace(".", "-")
        df     = yf.Ticker(yf_sym).history(
                    period="1d", interval="1m", prepost=True
                 )
        if momentum_screener(df, momentum_threshold=mom_thresh):
            passed.append(symbol)
    st.subheader("Tickers Passing Momentum")
    st.write(passed or "No tickers passed. Try a lower threshold!")
