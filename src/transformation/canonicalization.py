from rdkit import Chem

from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


canonical_schema = StructType([
    StructField(
        "canonical_smiles",
        StringType(),
        True
    ),
    StructField(
        "inchikey",
        StringType(),
        True
    )
])


@udf(canonical_schema)
def canonicalize_udf(smiles):

    if smiles is None:
        return {
            "canonical_smiles": None,
            "inchikey": None
        }

    try:
        mol = Chem.MolFromSmiles(smiles)

        if mol is None:
            return {
                "canonical_smiles": None,
                "inchikey": None
            }

        canonical_smiles = Chem.MolToSmiles(
            mol,
            canonical=True
        )

        inchikey = Chem.MolToInchiKey(mol)

        return {
            "canonical_smiles": canonical_smiles,
            "inchikey": inchikey
        }

    except Exception:
        return {
            "canonical_smiles": None,
            "inchikey": None
        }