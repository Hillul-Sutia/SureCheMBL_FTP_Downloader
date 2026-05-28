from src.downloader.ftp_client import FTPClient
from src.downloader.downloader import Downloader
from src.spark.session import create_spark

def run():

    spark = create_spark()

    ftp_client = FTPClient("ftp.ebi.ac.uk")

    ftp = ftp_client.connect()

    files = ftp_client.list_files(
        "/pub/databases/chembl/SureChEMBL/data"
    )

    downloader = Downloader(ftp)

    first_file = files[0]

    downloader.download_file(
        first_file,
        f"data/raw/{first_file}"
    )

    print("Pipeline completed.")

    ftp_client.close()

    spark.stop()

if __name__ == "__main__":

    run()