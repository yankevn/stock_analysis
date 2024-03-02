# pretty-print python data structures
from pprint import pprint

# for parsing all the tables present 
# on the website
from html_table_parser import HTMLTableParser

# for converting the parsed data in a
# pandas dataframe
import pandas as pd

from request import url_get_contents


def scrape_stock(url):
    # Opens a website and read its
    # binary contents (HTTP Response Body)
    
    # defining the html contents of a URL.
    xhtml = url_get_contents(url).decode('utf-8')

    # Defining the HTMLTableParser object
    p = HTMLTableParser()
    
    # feeding the html contents in the
    # HTMLTableParser object
    p.feed(xhtml)

    return p
