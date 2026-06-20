from pyspark.sql.functions import (
    col,
    concat_ws
)

from src.transformation.canonicalization import (
    canonical_smiles_udf,
    inchikey_udf
)


def create_database_id(df, id_columns):
    """
    Creates a single Database_ID column.

    If only one ID column exists:
        Database_ID = that column

    If multiple ID columns exist:
        Database_ID = comma-separated values
    """

    if not id_columns:
        raise ValueError(
            "No database ID columns found."
        )

    if len(id_columns) == 1:

        return df.withColumn(
            "Database_ID",
            col(id_columns[0])
        )

    return df.withColumn(
        "Database_ID",
        concat_ws(",", *id_columns)
    )


def add_canonical_smiles(
    df,
    smiles_column
):
    """
    Generates canonical SMILES.
    """

    return df.withColumn(
        "Canonical_SMILES",
        canonical_smiles_udf(
            col(smiles_column)
        )
    )


def add_inchikey(
    df,
    smiles_column
):
    """
    Generates InChIKey from SMILES.
    """

    return df.withColumn(
        "InChIKey",
        inchikey_udf(
            col(smiles_column)
        )
    )


def standardize_dataset(
    df,
    id_columns,
    smiles_column
):
    """
    Complete standardization workflow.
    """

    df = create_database_id(
        df,
        id_columns
    )

    df = add_canonical_smiles(
        df,
        smiles_column
    )

    df = add_inchikey(
        df,
        smiles_column
    )

    return df.select(
        "Database_ID",
        "Canonical_SMILES",
        "InChIKey"
    )