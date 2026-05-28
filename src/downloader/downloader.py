import os

class Downloader:

    def __init__(self, ftp):

        self.ftp = ftp

    def download_file(self, remote_file, local_path):

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with open(local_path, "wb") as f:

            self.ftp.retrbinary(
                f"RETR {remote_file}",
                f.write
            )

        print(f"Downloaded: {remote_file}")