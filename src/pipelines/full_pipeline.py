import os
import json

from src.spark.session import create_spark

from src.downloader.ftp_client import FTPClient
from src.downloader.downloader import Downloader

from src.spark.parquet_io import read_parquet
from src.discovery.chemical_columns import identify_chemical_columns
from src.discovery.id_detection import get_database_ids #, detect_id_columns
from src.transformation.standardization import standardize_dataset


from src.utils.logger import get_logger

logger = get_logger(__name__)

def run_pipeline():
    spark = None
    ftp_client = None
    try:
        
        # --------------------------- spark session creation --------------------------- # 
        spark = create_spark()
        # ------------------------------------------------------------------------------ #

        
        # ----------------------- FTP Address and Connection --------------------------- #
        
        ftp_dir = "/pub/databases/chembl/SureChEMBL/bulk_data"

        ftp_client = FTPClient("ftp.ebi.ac.uk")
        ftp = ftp_client.connect()
        
        # ------------------------------------------------------------------------------ #
        

        # ------------------- listing directories/folders in ftp address --------------- # 
        date_dirs = ftp_client.list_files(ftp_dir)

        logger.info(f"Date folders: {len(date_dirs)}", )
        logger.info(date_dirs[:10])
        # ------------------------------------------------------------------------------ #


        # ----------------------- Selecting directory in ftp --------------------------- #
        latest_date_dir = sorted(date_dirs)[-1]
        
        latest_folder = f"{ftp_dir}/{latest_date_dir}"
        # ------------------------------------------------------------------------------ #

        # --------------- selecting single file from ftp directory --------------------- #
        
        files = ftp_client.list_files(latest_folder)

        logger.info(f"Files in latest release {latest_date_dir}:")
        logger.info(files)

        parquet_files = [
            f for f in files
            if f.endswith("compounds.parquet")
        ]

        if not parquet_files:
            raise FileNotFoundError(
                f"No compounds.parquet found in {latest_folder}"
            )

        first_file = parquet_files[0]

        # ------------------------------------------------------------------------------ #

        # --------------------------------- File Downloading --------------------------- #
        remote_file = f"{latest_folder}/{first_file}"

        local_file  = (
            f"data/raw/{latest_date_dir}.parquet"
        )
        # downloader = Downloader(ftp)

        # downloader.download_file(
        #     remote_file,
        #     local_file
        # )
        # ------------------------------------------------------------------------------ #

        # ---------------------- pyspark load parquet file ----------------------------- #
        df = read_parquet(
            spark,
            local_file
        )

        logger.info(f"Columns: {df.columns}")
        # ------------------------------------------------------------------------------ #

        # ------------------------ sampling to DataFrame ------------------------------- #
        sample_df = (
            df
            .limit(10000)
            .toPandas()
        )
        # ------------------------------------------------------------------------------ #

        # ------------ sampling to DataFrame to find chemical data columns ------------- #
        chemical_result = identify_chemical_columns(
            sample_df
        )

        logger.info(f"Chemical columns found: {chemical_result}")
        # ------------------------------------------------------------------------------ #

        # ------------------- Excluding chemical data columns -------------------------- #
        excluded_cols = set()

        for key in ["SMILES", "InChI", "InChIKey", "URL"]:
            excluded_cols.update(chemical_result[key])

        candidate_id_cols = [
            col for col in df.columns
            if col not in excluded_cols
        ]

        logger.info("Excluded columns:", excluded_cols)
        logger.info("Candidate ID columns:", candidate_id_cols)
        # ------------------------------------------------------------------------------ #

        # ----------------------- Find database ID column ------------------------------ #
        id_columns = get_database_ids(
            df,
            candidate_id_cols
        )

        logger.info(f"Detected ID columns: {id_columns}")
        # ------------------------------------------------------------------------------ #

        # ----------------------- Canonical Transformation ----------------------------- #

        smiles_column = chemical_result["SMILES"][0]

        standardized_df = standardize_dataset(
            df,
            id_columns,
            smiles_column
        )

        # standardized_df.show(5, truncate=False)
        
        standardized_df.write.mode("overwrite").parquet(
            f"data/processed/{latest_date_dir}.parquet"
        )
        
        output_path = "data/processed/latest.parquet"
        standardized_df = spark.read.parquet(output_path)
        latest_date_dir = 'latest'

        total_rows = standardized_df.count()

        null_smiles = standardized_df.filter(
            standardized_df.canonical_smiles.isNull()
        ).count()

        null_ids = standardized_df.filter(
            standardized_df.database_id.isNull()
        ).count()

        # metadata = {
        #     "release": latest_date_dir,
        #     "rows": total_rows,
        #     "null_smiles": null_smiles,
        #     "null_ids": null_ids
        # }

        metadata = {
            "release": latest_date_dir,
            "raw_file": local_file,
            "processed_file": f"data/processed/{latest_date_dir}.parquet",
            "release": latest_date_dir,
            "chemical_columns": chemical_result,
            "id_columns": id_columns,
            "rows": total_rows,
            "null_smiles": null_smiles,
            "null_ids": null_ids
        }

        os.makedirs("data/metadata", exist_ok=True)

        metadata_path = f"data/metadata/{latest_date_dir}.json"

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)

        standardized_df = standardized_df.dropna(how="any")

        logger.info("Pipeline completed.")

    finally:
        if ftp_client:
            ftp_client.close()
        if spark:
            spark.stop()

# if __name__ == "__main__":
#     run()
