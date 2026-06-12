from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from rdkit import Chem
from rdkit.Chem import Descriptors


spark = (
    SparkSession.builder
    .appName("RDKit-Spark-Test")
    .master("local[*]")
    .getOrCreate()
)

# Sample SMILES
data = [
    ("CCO",),          # Ethanol
    ("CC(=O)O",),      # Acetic acid
    ("c1ccccc1",),     # Benzene
]

df = spark.createDataFrame(data, ["smiles"])


def molecular_weight(smiles):
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    return float(Descriptors.MolWt(mol))


mw_udf = udf(molecular_weight, FloatType())

result = df.withColumn(
    "molecular_weight",
    mw_udf(df.smiles)
)

result.show(truncate=False)

spark.stop()