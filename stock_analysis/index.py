import pandas as pd
from pprint import pprint
import re

from dynamodb import (
    create_tables_if_not_exist,
    put_pandas_dataframe,
    PAGE_TO_DB,
)
from scrape import scrape_stock


STOCK_TICKERS_FILE = "stock_symbols.txt"
stock_tickers = []
with open(STOCK_TICKERS_FILE) as infile:
    stock_tickers = infile.read().splitlines()


create_tables_if_not_exist()

for stock_ticker in stock_tickers:
    for page_type in PAGE_TO_DB.keys():
        url = f"https://stockanalysis.com/stocks/{stock_ticker}/financials/{page_type}"
        # URL needs to end with '/', otherwise permanent redirect
        if url[-1] != '/':
            url += '/'
        p = scrape_stock(url)
        df = pd.DataFrame(p.tables[0])

        # Transpose and drop the paywalled years
        df = df.T

        # Make the top row the header
        new_header = df.iloc[0]
        df = df[1:]
        df.columns = new_header
        df.columns.name = ""

        # Drop the "Current" year row
        if df.iloc[0]["Year"] == "Current":
            df = df[1:]
        if df.iloc[-1].iloc[1] == "Upgrade":
            df = df[:-1]

        df = df.set_index("Year")

        # Make header names database-friendly
        new_header = list(df.columns)
        new_header = [header.replace("/", "over") for header in new_header]
        new_header = [header.replace(" ", "_") for header in new_header]
        new_header = [re.sub(r"[()]", "", header) for header in new_header]
        df.columns = new_header

        # df = df.replace('-', 0)

        def df_series_to_float(series):
            series = series.str.replace(',', '')
            dashes = series.str.fullmatch('-')
            series[dashes] = "0"
            percentages = series.str.contains('%')
            series[percentages] = series[percentages].str.replace('%', '')
            series = series.astype(float)
            series[percentages] = series[percentages] / 100
            return series
        df = df.apply(df_series_to_float, axis=1)
        put_pandas_dataframe(stock_ticker, PAGE_TO_DB[page_type], df)
