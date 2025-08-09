import pandas as pd
import requests
from io import StringIO

# Fetch the S&P 500 companies from Wikipedia
stocks_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

response = requests.get(stocks_url, timeout=10)
if response.status_code == 200:
    print("Page loaded successfully")
else:
    print(f"Failed to load page: {response.status_code}")
    response.raise_for_status()

html_content = StringIO(response.text)
table = pd.read_html(html_content)
print(table[0].columns.tolist())
df = table[0][['Symbol', 'Security']]
df.columns = ["Ticker", "Name"]
df.to_csv("stock_tickers.csv", index=False)

# print(df.tail())