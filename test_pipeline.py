import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

def test_date_normalization(spark):
    schema = StructType([StructField("date_col", StringType(), True)])
    data = [("2023-01-01",), ("01/01/2023",)]
    df = spark.createDataFrame(data, schema)
    
    result = df.withColumn("clean_date", F.coalesce(
        F.to_date(F.col("date_col"), "yyyy-MM-dd"),
        F.to_date(F.col("date_col"), "dd/MM/yyyy")
    ))
    
    dates = result.select("clean_date").collect()
    assert dates[0][0] == dates[1][0]

def test_net_amount_logic(spark):
    schema = StructType([
        StructField("total_amount", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True)
    ])
    data = [(100.0, 15.0)]
    df = spark.createDataFrame(data, schema)
    
    result = df.withColumn("net_amount", F.col("total_amount") * (1 - F.col("discount_pct") / 100))
    val = result.select("net_amount").first()[0]
    assert val == 85.0
