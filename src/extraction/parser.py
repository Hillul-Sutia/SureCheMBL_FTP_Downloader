import gzip

def read_gzip_text(file_path):

    with gzip.open(file_path, "rt", encoding="utf-8") as f:

        return f.read()