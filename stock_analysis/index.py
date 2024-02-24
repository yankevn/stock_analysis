from pprint import pprint
import pandas as pd

from model import get_db_client
from scrape import scrape_stock

# income, balance sheet, and ratios
pages = ['', 'balance-sheet', 'ratios']

def test():
    # Connect to database
    client = get_db_client()
    print(client.list_tables())
test()

# stock_symbol = "GOOGL"
# for page_type in pages:
#     url = f"https://stockanalysis.com/stocks/{stock_symbol}/financials/{page_type}"
#     p = scrape_stock(url)
#     df = pd.DataFrame(p.tables[0])

#     pprint(df)
