from src.utils.spark_session import get_spark
from pyspark.sql import functions as F
from loguru import logger
import os
from datetime import datetime

BRONZE_PATH = "lakehouse/bronze"
RAW_PATH    = "data/raw"


SOURCE_TABLES = {
    "customers":        "raw_customers",
    "merchants":        "raw_merchants",
    "payment_methods":  "raw_payment_methods",
    "transactions":     "raw_transactions",
}


def load_csv_to_bronze(spark, source_name: str, load_date: str):
    """
    Reads a CSV file and writes it to Bronze Delta table.
    
    source_name → key from SOURCE_TABLES dict (e.g. "customers")
    load_date   → partition value (e.g. "2024-01-15")
    """
    
    csv_path    = f"{RAW_PATH}/{source_name}.csv"
    table_name  = SOURCE_TABLES[source_name]
    delta_path  = f"{BRONZE_PATH}/{table_name}"

    logger.info(f"Loading {source_name}.csv → bronze/{table_name}")

#step 1 = Read csv from source
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  
        .csv(csv_path)
    )

#step 2 = Add required columns like source_file, timestamp & date.
    df = (
        df
        .withColumn("load_timestamp", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        .withColumn("source_file",    F.lit(f"{source_name}.csv"))
        .withColumn("load_date",      F.lit(load_date))
    )

#step 3 = Load data to data lake bronze layer
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("load_date")
        .save(delta_path)
    )

    row_count = spark.read.format("delta").load(delta_path).count()
    logger.success(f"✅ bronze/{table_name} → {row_count:,} rows loaded")

#step 4: Write Main Function
def main():
    logger.info("Starting Bronze Layer ingestion")
    
    spark     = get_spark(app_name="BronzeLoader")
    load_date = datetime.now().strftime("%Y-%m-%d")

    try:
        for source_name in SOURCE_TABLES.keys():
            load_csv_to_bronze(spark, source_name, load_date)

        logger.success("✅ Bronze Layer ingestion complete!")

    except Exception as e:
        logger.error(f"❌ Bronze ingestion failed: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()