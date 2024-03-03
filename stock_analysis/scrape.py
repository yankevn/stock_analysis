# pretty-print python data structures
from pprint import pprint

# for parsing all the tables present 
# on the website
from html_table_parser import HTMLTableParser

# for converting the parsed data in a
# pandas dataframe
import pandas as pd
import urllib.request


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


def url_get_contents(url):

    # Opens a website and read its
    # binary contents (HTTP Response Body)

    # Add headers for authentication and stuff
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
    'Accept-Encoding': 'utf-8',
    'Accept-Language': 'en-US,en;q=0.8',
    'Connection': 'keep-alive'}

    #making request to the website
    req = urllib.request.Request(url=url, headers=hdr)
    f = urllib.request.urlopen(req)

    #reading contents of the website
    return f.read()