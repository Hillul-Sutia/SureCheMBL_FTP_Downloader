import re

from rdkit import Chem
from rdkit import RDLogger

RDLogger.DisableLog("rdApp.*")


def is_smiles(value):
    try:
        mol = Chem.MolFromSmiles(str(value), sanitize=False)
        return mol is not None
    except:
        return False


def is_inchi(value):
    pattern = re.compile(r"^InChI=\d+[A-Za-z]?/.*$")
    return bool(pattern.match(str(value)))


def is_inchikey(value):
    pattern = re.compile(r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$")
    return bool(pattern.match(str(value)))


def is_url(value):
    pattern = re.compile(
        r"^(https?://)?"
        r"([\da-z\.-]+)\.([a-z\.]{2,6})"
        r"(:\d+)?"
        r"(/[\w\.-]*)*"
        r"(\?[^\s]*)?$"
    )
    return bool(pattern.match(str(value)))


def identify_chemical_columns(df):

    result = {
        "ID": [],
        "SMILES": [],
        "InChI": [],
        "InChIKey": [],
        "URL": []
    }

    df = df.copy()

    # Remove empty columns
    df.dropna(axis=1, how="all", inplace=True)

    for column in df.columns:

        values = df[column].dropna()

        if len(values) == 0:
            continue

        unique_values = values.unique()

        smiles_count = sum(
            is_smiles(v)
            for v in unique_values
        )

        if smiles_count > len(unique_values) * 0.5:
            result["SMILES"].append(column)
            continue

        if values.apply(is_inchi).any():
            result["InChI"].append(column)
            continue

        if values.apply(is_inchikey).any():
            result["InChIKey"].append(column)
            continue

        if values.apply(is_url).any():
            result["URL"].append(column)
            continue

        # Candidate ID column
        if values.is_unique and values.notna().all():
            result["ID"].append(column)

    return result