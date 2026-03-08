from src.utils.spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DecimalType, DateType
)
from loguru import logger
from datetime import datetime
from delta.tables import DeltaTable

# Paths
BRONZE_PATH = "lakehouse/bronze"
SILVER_PATH = "lakehouse/silver"

def transform_customers(spark):
    logger.info("Transforming customers Bronze → Silver")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/raw_customers")

    # Step 1: Cast types
    df = (
        df
        .withColumn("wallet_limit",  F.col("wallet_limit").cast(IntegerType()))
        .withColumn("created_date",  F.col("created_date").cast(DateType()))
        .withColumn("is_active",     F.lit(1).cast(IntegerType()))
    )

    # Step 2: Clean strings
    df = (
        df
        .withColumn("full_name",         F.trim(F.col("full_name")))
        .withColumn("email",             F.trim(F.lower(F.col("email"))))
        .withColumn("city",              F.trim(F.initcap(F.col("city"))))
        .withColumn("state",             F.trim(F.upper(F.col("state"))))
        .withColumn("kyc_tier",          F.trim(F.upper(F.col("kyc_tier"))))
        .withColumn("customer_segment",  F.trim(F.upper(F.col("customer_segment"))))
    )

    # Step 3: Handle nulls
    df = df.fillna({
        "city":             "UNKNOWN",
        "state":            "UNKNOWN",
        "customer_segment": "UNKNOWN",
    })

    # Step 4: Drop rows where key column is NULL
    before = df.count()
    df = df.dropna(subset=["customer_id"])
    after = df.count()
    dropped = before - after

    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with NULL customer_id")
    else:
        logger.info("No NULL customer_ids found")

    # Step 5: Remove duplicates based on natural key
    df = df.dropDuplicates(["customer_id"])

    # Step 6: Add silver metadata
    df = df.withColumn("silver_loaded_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Step 7: Write to Silver
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{SILVER_PATH}/customers")
    )

    logger.success(f"✅ silver/customers → {df.count():,} rows")

def transform_merchants(spark):
    logger.info("Transforming merchants Bronze → Silver")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/raw_merchants")

    # Step 1: Cast types
    df = (
        df
        .withColumn("is_active",       F.col("is_active").cast(IntegerType()))
        .withColumn("onboarded_date",  F.col("onboarded_date").cast(DateType()))
    )

    # Step 2: Clean strings
    df = (
        df
        .withColumn("merchant_name",  F.trim(F.col("merchant_name")))
        .withColumn("category",       F.trim(F.upper(F.col("category"))))
        .withColumn("sub_category",   F.trim(F.upper(F.col("sub_category"))))
        .withColumn("city",           F.trim(F.initcap(F.col("city"))))
        .withColumn("state",          F.trim(F.upper(F.col("state"))))
    )

    # Step 3: Handle nulls
    df = df.fillna({
        "city":     "UNKNOWN",
        "state":    "UNKNOWN",
        "is_active": 1,
    })

    # Step 4: Drop rows where key column is NULL
    before = df.count()
    df = df.dropna(subset=["merchant_id"])
    after  = df.count()
    dropped = before - after

    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with NULL merchant_id")
    else:
        logger.info("No NULL merchant_ids found")

    # Step 5: Remove duplicates
    df = df.dropDuplicates(["merchant_id"])

    # Step 6: Add silver metadata
    df = df.withColumn("silver_loaded_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Step 7: Write to Silver
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{SILVER_PATH}/merchants")
    )

    logger.success(f"✅ silver/merchants → {df.count():,} rows")
  
def transform_payment_methods(spark):
    logger.info("Transforming payment_methods Bronze → Silver")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/raw_payment_methods")

    # Step 1: Cast types
    df = df.withColumn("is_active", F.col("is_active").cast(IntegerType()))

    # Step 2: Clean strings
    df = (
        df
        .withColumn("method_code", F.trim(F.upper(F.col("method_code"))))
        .withColumn("method_name", F.trim(F.col("method_name")))
        .withColumn("method_type", F.trim(F.upper(F.col("method_type"))))
    )

    # Step 3: Drop NULL keys
    before  = df.count()
    df      = df.dropna(subset=["method_id"])
    dropped = before - df.count()

    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with NULL method_id")

    # Step 4: Remove duplicates
    df = df.dropDuplicates(["method_id"])

    # Step 5: Silver metadata
    df = df.withColumn("silver_loaded_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Step 6: Write to Silver
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{SILVER_PATH}/payment_methods")
    )

    logger.success(f"✅ silver/payment_methods → {df.count():,} rows")

def transform_transactions(spark):
    logger.info("Transforming transactions Bronze → Silver")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/raw_transactions")

    # Step 1: Cast types
    df = (
        df
        .withColumn("txn_amount",      F.col("txn_amount").cast(DecimalType(10,2)))
        .withColumn("platform_fee",    F.col("platform_fee").cast(DecimalType(10,2)))
        .withColumn("cashback_amount", F.col("cashback_amount").cast(DecimalType(10,2)))
        .withColumn("is_fraud_flag",   F.col("is_fraud_flag").cast(IntegerType()))
        .withColumn("txn_date",        F.col("txn_date").cast(DateType()))
    )

    # Step 2: Clean strings
    df = (
        df
        .withColumn("txn_status",     F.trim(F.upper(F.col("txn_status"))))
        .withColumn("method_code",    F.trim(F.upper(F.col("method_code"))))
        .withColumn("failure_reason", F.trim(F.upper(F.col("failure_reason"))))
    )

    # Step 3: Handle nulls in measures
    df = df.fillna({
        "txn_amount":      0.0,
        "platform_fee":    0.0,
        "cashback_amount": 0.0,
        "is_fraud_flag":   0,
    })

    # Step 4: Data quality checks
    before = df.count()

    # Drop NULL key rows
    df = df.dropna(subset=["transaction_id", "customer_id", "merchant_id"])

    # Drop negative amounts — invalid business data
    df = df.filter(F.col("txn_amount") >= 0)

    # Drop invalid statuses
    valid_statuses = ["SUCCESS", "FAILED", "PENDING"]
    df = df.filter(F.col("txn_status").isin(valid_statuses))

    after   = df.count()
    dropped = before - after

    if dropped > 0:
        logger.warning(f"Dropped {dropped} invalid transaction rows")
    else:
        logger.info("All transaction rows passed quality checks")

    # Step 5: Add derived column
    df = df.withColumn(
        "net_revenue",
        F.col("platform_fee") - F.col("cashback_amount")
    )

    # Step 6: Add partition columns for performance
    df = (
        df
        .withColumn("txn_year",  F.year("txn_date"))
        .withColumn("txn_month", F.month("txn_date"))
    )

    # Step 7: Remove duplicates
    df = df.dropDuplicates(["transaction_id"])

    # Step 8: Silver metadata
    df = df.withColumn("silver_loaded_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Step 9: Write to Silver partitioned by year + month
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("txn_year", "txn_month")
        .save(f"{SILVER_PATH}/transactions")
    )

    logger.success(f"✅ silver/transactions → {df.count():,} rows")

def apply_customer_updates(spark):
    logger.info("Applying customer updates to Silver")

    updates_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv("data/raw/customer_updates.csv")
    )


    updates_df = (
        updates_df
        .withColumn("wallet_limit", F.col("wallet_limit").cast(IntegerType()))
        .withColumn("created_date", F.col("created_date").cast(DateType()))
        .withColumn("updated_date", F.col("updated_date").cast(DateType()))
    )


    updates_df = (
        updates_df
        .withColumn("kyc_tier",         F.trim(F.upper(F.col("kyc_tier"))))
        .withColumn("customer_segment", F.trim(F.upper(F.col("customer_segment"))))
        .withColumn("city",             F.trim(F.initcap(F.col("city"))))
        .withColumn("state",            F.trim(F.upper(F.col("state"))))
    )


    updates_df = updates_df.withColumn(
        "silver_loaded_at",
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    silver_df = spark.read.format("delta").load(f"{SILVER_PATH}/customers")

    silver_table = DeltaTable.forPath(spark, f"{SILVER_PATH}/customers")

    (
        silver_table.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.customer_id = source.customer_id"
        )
        .whenMatchedUpdate(
            set={
                "target.kyc_tier":          "source.kyc_tier",
                "target.wallet_limit":      "source.wallet_limit",
                "target.customer_segment":  "source.customer_segment",
                "target.city":              "source.city",
                "target.state":             "source.state",
                "target.silver_loaded_at":  "source.silver_loaded_at",
            }
        )
        .execute()
    )

    updated_count = updates_df.count()
    logger.success(f"✅ Applied {updated_count:,} customer updates to Silver")


def main():
    logger.info("Starting Silver Layer transformations")

    spark = get_spark(app_name="SilverTransform")

    try:
        transform_customers(spark)
        transform_merchants(spark)
        transform_payment_methods(spark)
        transform_transactions(spark)
        apply_customer_updates(spark)

        logger.success("✅ Silver Layer transformations complete!")

    except Exception as e:
        logger.error(f"❌ Silver transformation failed: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()