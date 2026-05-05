import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


def download_file(file_url, local_path, timeout, retries):
    if os.path.exists(local_path):
        logging.info(f"Skipping existing: {local_path}")
        return

    for attempt in range(retries):
        try:
            with requests.get(file_url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)

            logging.info(f"Downloaded: {local_path}")
            return

        except Exception as e:
            logging.warning(f"Retry {attempt+1} failed for {file_url}: {e}")
            time.sleep(2)

    logging.error(f"Failed permanently: {file_url}")


def download_files_parallel(tasks, num_workers, timeout, retries):
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(download_file, url, path, timeout, retries)
            for url, path in tasks
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Task failed: {e}")