import datetime
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
import urllib.request
import re
import json

def get_closest_snapshot_url(content):
    
    re = json.loads(content)
    
    if re["archived_snapshots"] == {}:
        return False, None

    if int(re["archived_snapshots"]["closest"]["status"]) != 200:
        return False, None
    
    if not re["archived_snapshots"]["closest"]["available"]:
        return False, None

    return False, re["archived_snapshots"]["closest"]["url"]


def get_wb_available_url(web_url, timestamp):
    return "https://archive.org/wayback/available?url=" + str(web_url) + "&timestamp=" + timestamp

def get_actual_url(waybackmachine_url):
    # remove the "http://web.archive.org/web/20230921000236/" prefix
    return re.match(r"^(?:[^\/]*\/){5}(.*)$", waybackmachine_url).group(1)

def get_wb_timestamp(waybackmachine_url):
    # get timestamp from the url
    # e.g: "20230921000236" from "http://web.archive.org/web/20230921000236/"
    return re.match(r"^(?:[^\/]*\/){4}([0-9]*)\/.*$", waybackmachine_url).group(1)

def to_wb_timestamp(date):
    return date.strftime("%Y%m%d")
