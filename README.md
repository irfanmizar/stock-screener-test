Stock Screener
A simple stock screener built with Python, yfinance, and Streamlit. The app fetches and analyzes stock data, then displays results in an interactive dashboard.

Key Files

dashboard.py – Runs the Streamlit UI and displays results.
screener.py – Contains data processing functions (e.g., Slice_window, compute_metrics).
filters/ – Additional filtering modules for stock selection.
testing files/ - Just some other files that I have used when creating the program initially. Do not open.

After cloning the repository:

Create and activate a virtual environment

    python -m venv venv
    source venv/bin/activate   # Mac/Linux
    venv\Scripts\activate      # Windows

To Run the App:
In the terminal, type
    streamlit run dashboard.py

The program will then pop up as a localhost program and to terminate it, just press Ctrl+C in the terminal