import pandas as pd

def momentum_screener(df, threshold=0.5):
    """
    Filter stocks based on momentum.
    Price momentum is stock price change over a period of time by threshhold%.
    """
    if df.empty:
        return False
    # price at the start is the first row of close and end is the last row of close
    start_price = df['Close'].iloc[0]
    end_price = df['Close'].iloc[-1]
    price_change = abs((end_price - start_price) / start_price * 100)
    return price_change > threshold