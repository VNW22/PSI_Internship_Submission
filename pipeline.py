import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

def main():
    # Initialize Spark Session with comprehensive flags for modern Java versions
    builder = SparkSession.builder.appName("PSI_EComm_Pipeline")
    
    # These flags are required for Java 17+ (and Java 26) to work with Spark
    jvm_flags = [
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    ]
    
    # Apply flags to both driver and executor
    flags_str = " ".join(jvm_flags)
    spark = builder \
        .config("spark.driver.extraJavaOptions", flags_str) \
        .config("spark.executor.extraJavaOptions", flags_str) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    data_path = "data"
    output_path = "output"
    os.makedirs(f"{output_path}/rejected", exist_ok=True)

    # 1. Data Ingestion & Schema Enforcement
    def load_with_schema(file_name, schema):
        # Read without inferSchema to enforce our own
        df = spark.read.csv(f"{data_path}/{file_name}.csv", header=True)
        
        # Cast columns to specified types
        casted_df = df
        for field in schema.fields:
            casted_df = casted_df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            
        # Identify rejected rows (any nulls in key fields after casting)
        # For simplicity, we check all columns in the schema
        valid_df = casted_df.dropna(subset=[f.name for f in schema.fields])
        rejected_df = casted_df.subtract(valid_df)
        
        if rejected_df.count() > 0:
            rejected_df.write.mode("overwrite").csv(f"{output_path}/rejected/{file_name}")
            
        return valid_df

    cust_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("signup_date", StringType(), True),
        StructField("country", StringType(), True),
        StructField("customer_tier", StringType(), True),
        StructField("email", StringType(), True)
    ])
    
    items_schema = StructType([
        StructField("item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("category", StringType(), True)
    ])
    
    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True)
    ])
    
    ret_schema = StructType([
        StructField("return_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("return_date", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("refund_amount", DoubleType(), True)
    ])

    cust = load_with_schema("customers", cust_schema)
    items = load_with_schema("order_items", items_schema)
    orders = load_with_schema("orders", orders_schema)
    returns = load_with_schema("returns", ret_schema)

    # 2. Data Quality & Cleaning
    def clean_dates(df, col):
        return df.withColumn(col, F.coalesce(
            F.to_date(F.col(col), "yyyy-MM-dd"),
            F.to_date(F.col(col), "dd/MM/yyyy")
        )).dropna(subset=[col])

    cust = clean_dates(cust.dropDuplicates(), "signup_date") \
        .withColumn("customer_tier", F.lower(F.col("customer_tier")))
        
    orders = clean_dates(orders.dropDuplicates(), "order_date") \
        .withColumn("is_negative_amount", F.col("total_amount") < 0)
        
    items = items.dropDuplicates()
    returns = clean_dates(returns.dropDuplicates(), "return_date")

    # 3. Joins & Enrichment
    # Bonus B2: Broadcast Join for the smaller customers table
    orders_enriched = orders.join(F.broadcast(cust), "customer_id", "inner")
    
    # Anti-join for orphaned items
    orphans = items.join(orders, "order_id", "left_anti")
    orphans.write.mode("overwrite").csv(f"{output_path}/orphaned_items")

    final = orders_enriched.join(items, "order_id", "inner")
    final = final.withColumn("net_amount", F.col("total_amount") * (1 - F.col("discount_pct") / 100))

    # 4. Aggregations & Window Functions
    # Ranking by lifetime spend per country
    win_rank = Window.partitionBy("country").orderBy(F.desc("total_spend"))
    spend_df = final.groupBy("customer_id", "country").agg(F.sum("net_amount").alias("total_spend"))
    ranked_customers = spend_df.withColumn("rank", F.rank().over(win_rank))

    # 7-day rolling order count
    # Convert date to unix timestamp for range window
    final = final.withColumn("ts", F.unix_timestamp("order_date"))
    win_roll = Window.partitionBy("customer_id").orderBy("ts").rangeBetween(-6*24*60*60, 0)
    final = final.withColumn("rolling_7d_orders", F.count("order_id").over(win_roll)).drop("ts")

    # Share of revenue per category per month
    final = final.withColumn("month_str", F.date_format("order_date", "yyyy-MM"))
    win_month = Window.partitionBy("month_str")
    cat_month_rev = final.groupBy("month_str", "category").agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("cat_revenue"))
    cat_month_rev = cat_month_rev.withColumn("total_month_revenue", F.sum("cat_revenue").over(win_month))
    share_df = cat_month_rev.withColumn("revenue_share", F.col("cat_revenue") / F.col("total_month_revenue"))

    # 5. Return Analysis
    ret_en = returns.join(final, "order_id", "left")
    ret_en = ret_en.withColumn("refund_exceeds_order", F.col("refund_amount") > F.col("net_amount"))
    
    total_orders = orders.count()
    return_rates = ret_en.groupBy("category", "customer_tier").agg((F.count("return_id") / total_orders).alias("return_rate"))
    top_refund_customers = ret_en.groupBy("customer_id").agg(F.sum("refund_amount").alias("total_refunded")) \
        .orderBy(F.desc("total_refunded")).limit(10)

    # Bonus B3: DQ Gate
    if final.filter(F.col("customer_id").isNull()).count() > 0:
        raise Exception("DQ Gate Failure: NULL customer_id found in final dataset")

    # 6. Output & Partitioning
    final = final.withColumn("year", F.year("order_date")).withColumn("month", F.month("order_date"))
    final.write.partitionBy("year", "month").mode("overwrite").parquet(f"{output_path}/final_enriched_data")
    
    ranked_customers.write.mode("overwrite").csv(f"{output_path}/summaries/customer_ranks", header=True)
    share_df.write.mode("overwrite").csv(f"{output_path}/summaries/category_shares", header=True)
    return_rates.write.mode("overwrite").csv(f"{output_path}/summaries/return_rates", header=True)
    top_refund_customers.write.mode("overwrite").csv(f"{output_path}/summaries/top_refunds", header=True)

    # Bonus B4: Explain Plan
    print("Execution Plan for the final enriched dataset:")
    final.explain(mode="formatted")

if __name__ == "__main__":
    main()
