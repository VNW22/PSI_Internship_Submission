import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

def main():
    # Note: This script contains the PySpark logic for the assessment.
    # Due to local JVM compatibility issues (Java 26), this was not used for final execution.
    spark = SparkSession.builder \
        .appName("PSI_EComm_PySpark_Implementation") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    data_path = "data"
    output_path = "output_pyspark"

    # Schema Enforcement
    def load(file, schema):
        df = spark.read.csv(f"{data_path}/{file}.csv", header=True)
        for field in schema.fields:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
        valid = df.dropna(subset=[f.name for f in schema.fields])
        return valid

    cust_s = StructType([StructField("customer_id", StringType(), False), StructField("signup_date", StringType(), True), StructField("country", StringType(), True), StructField("customer_tier", StringType(), True)])
    orders_s = StructType([StructField("order_id", StringType(), False), StructField("customer_id", StringType(), False), StructField("order_date", StringType(), True), StructField("total_amount", DoubleType(), True), StructField("discount_pct", DoubleType(), True)])
    
    cust = load("customers", cust_s)
    orders = load("orders", orders_s)

    # Date cleaning and normalization
    orders = orders.withColumn("order_date", F.coalesce(F.to_date("order_date", "yyyy-MM-dd"), F.to_date("order_date", "dd/MM/yyyy")))

    # Bonus B2: Broadcast join
    enriched = orders.join(F.broadcast(cust), "customer_id", "inner")

    # Task 04: Window Functions
    win_roll = Window.partitionBy("customer_id").orderBy(F.unix_timestamp("order_date")).rangeBetween(-6*24*60*60, 0)
    enriched = enriched.withColumn("rolling_7d_orders", F.count("order_id").over(win_roll))

    # Task 06: Partitioned Output
    enriched.write.partitionBy(F.year("order_date"), F.month("order_date")).mode("overwrite").parquet(f"{output_path}/enriched_data")

if __name__ == "__main__":
    main()
