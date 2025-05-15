import pandas as pd

def screener(df, threshold=2.0):
    """
    Filter stocks based on momentum.
    
    Parameters:
    df (DataFrame): DataFrame containing stock data with 'Ticker' and 'Momentum' columns.
    threshold (float): Minimum momentum value to filter stocks.
    
    Returns:
    DataFrame: Filtered DataFrame with stocks that have momentum greater than the threshold.
    """
    # Ensure the 'Momentum' column is numeric
    df['Momentum'] = pd.to_numeric(df['Momentum'], errors='coerce')
    
    # Filter stocks based on momentum
    filtered_stocks = df[df['Momentum'] > threshold]
    
    return filtered_stocks[['Ticker', 'Name', 'Momentum']]
