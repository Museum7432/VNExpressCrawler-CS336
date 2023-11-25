import datetime
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
import urllib.request
import re

from urllib.parse import urlparse
from os.path import splitext

def get_thumbnail_url(content_soup):
    url = content_soup.find("meta",{"name":"twitter:image"}).get("content")

    return url

# def get_date_timestamp(content_soup):
#     return

def get_article_id(article_url):
    # get id from url
    # e.g 4675083 from "https://vnexpress.net/trang-phap-toi-khong-ham-danh-vong-4675083.html"
    return re.match(r"^.*-([0-9]*)\.html.*$", article_url).group(1)

def get_articles_links(frontpage_content_soup):
    titles = frontpage_content_soup.find_all(lambda tag: tag.name == 'a' and 'data-medium' in tag.attrs and tag["data-medium"].startswith("Item-"))
    links = []
    for title in titles:

        link = title.get("href")

        if not link or not link.endswith(".html"):
            continue
        
        links.append(link)
    return links


def get_ext(url):
    """Return the filename extension from url, or ''."""
    parsed = urlparse(url)
    root, ext = splitext(parsed.path)
    return ext  # or ext[1:] if you don't want the leading '.'
