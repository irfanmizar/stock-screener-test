import pandas as pd

def volume_spike_screener(df, volume_threshold):
    """
    Filter stocks based on volume spike.
    Volume spike is defined as the current volume being greater than the average volume by a certain percentage.
    """
    if df.empty:
        return False
    # calculate the average volume over the last 20 days
    avg_volume = df['Volume'].rolling(window=20).mean().iloc[-1]
    if (avg_volume == 0):
        return False
    current_volume = df['Volume'].iloc[-1]
    volume_change = abs((current_volume - avg_volume) / avg_volume * 100)
    return volume_change > volume_threshold