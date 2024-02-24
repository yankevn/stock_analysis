# Library for opening url and creating requests
import urllib.request

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