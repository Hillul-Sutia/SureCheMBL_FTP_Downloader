
import os
import requests
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool

BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/chembl/SureChEMBL/bulk_data/"
NUM_WORKERS = 8


def list_directories():
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.text, "html.parser")

    return [
        a.get("href").strip("/")
        for a in soup.find_all("a")
        if a.get("href").endswith("/") and a.get("href") != "../"
    ]


def list_files(month):
    url = BASE_URL + month + "/"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    return [
        url + a.get("href")
        for a in soup.find_all("a")
        if a.get("href").endswith(".parquet") and 'compounds' in a.get("href")
    ]


def download_file(args):
    file_url, local_path = args

    if os.path.exists(local_path):
        return

    try:
        with requests.get(file_url, stream=True) as r:
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        print(f"✔ {local_path}")
    except Exception as e:
        print(f"⚠️ Failed: {file_url} ({e})")


def download_month(month):
    print(f"\nProcessing {month}")
    os.makedirs(month, exist_ok=True)

    files = list_files(month)

    tasks = [
        (url, os.path.join(month, url.split("/")[-1]))
        for url in files
    ]

    pool = ThreadPool(NUM_WORKERS)
    pool.map(download_file, tasks)
    pool.close()
    pool.join()


if __name__ == "__main__":
    months = list_directories()

    for m in months:
        download_month(m)



"""import os
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/chembl/SureChEMBL/bulk_data/"

def list_directories():
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.text, "html.parser")

    dirs = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href.endswith("/") and href != "../":
            dirs.append(href.strip("/"))
    return dirs

def download_files(month):
    url = BASE_URL + month + "/"
    local_dir = month
    os.makedirs(local_dir, exist_ok=True)

    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    for link in soup.find_all("a"):
        file = link.get("href")

        if not file.endswith(".parquet") and 'compounds' not in file:
            continue

        file_url = url + file
        local_path = os.path.join(local_dir, file)

        if os.path.exists(local_path):
            continue

        print(f"Downloading {file_url}")

        with requests.get(file_url, stream=True) as r:
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)

if __name__ == "__main__":
    months = list_directories()

    for m in months:
        print(f"Processing {m}")
        download_files(m)
"""
