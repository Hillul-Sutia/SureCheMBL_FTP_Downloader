import os
from src.extractor import list_directories, list_files
from src.downloader import download_files_parallel
from src.utils import setup_logging, load_config


def run_pipeline():
    config = load_config()
    setup_logging()

    base_url = config["base_url"]
    download_dir = config["download_dir"]
    num_workers = config["num_workers"]
    file_filter = config["file_filter"]
    timeout = config["timeout"]
    retries = config["retries"]

    os.makedirs(download_dir, exist_ok=True)

    months = list_directories(base_url)

    all_tasks = []

    for month in months:
        print(f"Processing {month}")

        month_dir = os.path.join(download_dir, month)
        os.makedirs(month_dir, exist_ok=True)

        files = list_files(base_url, month, file_filter)

        tasks = [
            (url, os.path.join(month_dir, url.split("/")[-1]))
            for url in files
        ]

        all_tasks.extend(tasks)

    all_tasks = all_tasks[:4]
    download_files_parallel(all_tasks, num_workers, timeout, retries)

    # for month in months:
    #     print(f"Processing {month}")

    #     month_dir = os.path.join(download_dir, month)
    #     os.makedirs(month_dir, exist_ok=True)

    #     files = list_files(base_url, month, file_filter)

    #     tasks = [
    #         (url, os.path.join(month_dir, url.split("/")[-1]))
    #         for url in files
    #     ]

    #     download_files_parallel(tasks, num_workers, timeout, retries)


if __name__ == "__main__":
    run_pipeline()