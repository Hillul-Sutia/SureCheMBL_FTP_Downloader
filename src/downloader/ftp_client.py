from ftplib import FTP

class FTPClient:

    def __init__(self, host):
        self.host = host
        self.ftp = None

    def connect(self):

        self.ftp = FTP(self.host)
        self.ftp.login()

        return self.ftp

    def list_files(self, path):

        self.ftp.cwd(path)

        return self.ftp.nlst()

    def close(self):

        if self.ftp:
            self.ftp.quit()