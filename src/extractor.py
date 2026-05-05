import requests
from bs4 import BeautifulSoup


def list_directories(base_url):
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, "html.parser")

    return [
        a.get("href").strip("/")
        for a in soup.find_all("a")
        if a.get("href").endswith("/") and a.get("href") != "../"
    ]


def list_files(base_url, month, file_filter):
    url = f"{base_url}{month}/"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    return [
        url + a.get("href")
        for a in soup.find_all("a")
        if a.get("href").endswith(".parquet") and file_filter in a.get("href")
    ]