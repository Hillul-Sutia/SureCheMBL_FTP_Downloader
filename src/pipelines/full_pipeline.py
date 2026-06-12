import os
from src.downloader.ftp_client import FTPClient
from src.downloader.downloader import Downloader
from src.spark.session import create_spark


def run_pipeline():

    spark = create_spark()

    # FTP Address and Connection
    ftp_dir = "/pub/databases/chembl/SureChEMBL/bulk_data"

    ftp_client = FTPClient("ftp.ebi.ac.uk")
    ftp = ftp_client.connect()

    date_dirs = ftp_client.list_files(ftp_dir)

    latest_date_dir = sorted(date_dirs)[-1]

    print("Date folders:", len(date_dirs))
    print(date_dirs[:10])

    latest_folder = f"{ftp_dir}/{latest_date_dir}"

    files = ftp_client.list_files(latest_folder)

    print(f"Files in latest release {latest_date_dir}:")
    print(files)

    parquet_files = [
        f for f in files
        if f.endswith("compounds.parquet")
    ]

    if not parquet_files:
        raise FileNotFoundError(
            f"No compounds.parquet found in {latest_folder}"
        )

    first_file = parquet_files[0]

    remote_file = f"{latest_folder}/{first_file}"

    downloader = Downloader(ftp)

    downloader.download_file(
        remote_file,
        f"data/{latest_date_dir}.parquet"
    )

    print("Pipeline completed.")

    ftp_client.close()
    spark.stop()

# if __name__ == "__main__":
#     run()
