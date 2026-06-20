from rdkit import Chem
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def canonical_smiles_udf(smiles):

    if smiles is None:
        return None

    mol = Chem.MolFromSmiles(smiles)

    if mol:
        return Chem.MolToSmiles(
            mol,
            canonical=True
        )

    return None


@udf(StringType())
def inchikey_udf(smiles):

    if smiles is None:
        return None

    mol = Chem.MolFromSmiles(smiles)

    if mol:
        return Chem.MolToInchiKey(mol)

    return None