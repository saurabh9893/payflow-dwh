from src.utils.spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType
from delta.tables import DeltaTable
from loguru import logger
from datetime import datetime
import os

SILVER_PATH = "lakehouse/silver"
GOLD_PATH   = "lakehouse/gold"


def load_dims(spark):
    """
    Load all dimension tables.
    For dim_customer we only need current rows for the join.
    We also need historical rows for point-in-time join.
    """
    dim_merchant = spark.read.format("delta").load(f"{GOLD_PATH}/dim_merchant")
    dim_payment  = spark.read.format("delta").load(f"{GOLD_PATH}/dim_payment_method")
    dim_date     = spark.read.format("delta").load(f"{GOLD_PATH}/dim_date")
    dim_customer = spark.read.format("delta").load(f"{GOLD_PATH}/dim_customer")

    logger.info(f"dim_customer rows : {dim_customer.count():,}")
    logger.info(f"dim_merchant rows : {dim_merchant.count():,}")
    logger.info(f"dim_payment rows  : {dim_payment.count():,}")
    logger.info(f"dim_date rows     : {dim_date.count():,}")

    return dim_customer, dim_merchant, dim_payment, dim_date

def build_fact_transaction(spark):
    logger.info("Building fact_transaction")

    #Step 1: Load Silver transactions
    txn_df = spark.read.format("delta").load(f"{SILVER_PATH}/transactions")

    #Step 2: Load dimensions
    dim_customer, dim_merchant, dim_payment, dim_date = load_dims(spark)

    #Step 3: Join dim_date
    #Convert txn_date to date_key integer format
    txn_df = txn_df.withColumn(
        "date_key",
        F.date_format("txn_date", "yyyyMMdd").cast(IntegerType())
    )

    # Select only needed columns from dim_date
    dim_date_slim = dim_date.select("date_key")

    txn_df = txn_df.join(
        dim_date_slim,
        on="date_key",
        how="left"
    )

    #Step 4: Join dim_merchant (SCD1 — simple join)
    dim_merchant_slim = dim_merchant.select(
        "merchant_key",
        "merchant_id"
    )

    txn_df = txn_df.join(
        dim_merchant_slim,
        on="merchant_id",
        how="left"
    )

    #Step 5: Join dim_payment_method (SCD1 — simple)
    dim_payment_slim = dim_payment.select(
        "payment_method_key",
        "method_code"
    )

    txn_df = txn_df.join(
        dim_payment_slim,
        on="method_code",
        how="left"
    )

    # Step 6: Join dim_customer (SCD2 — point-in-time)
    # This is the most important join
    # Match on customer_id AND transaction date falls within
    # the customer version's effective date range

    dim_customer_slim = dim_customer.select(
        "customer_key",
        "customer_id",
        "effective_start",
        "effective_end",
        "is_current"
    )

    txn_df = txn_df.join(
        dim_customer_slim,
        on=(
            (txn_df["customer_id"] == dim_customer_slim["customer_id"]) &
            (txn_df["txn_date"] >= dim_customer_slim["effective_start"]) &
            (
                dim_customer_slim["effective_end"].isNull() |
                (txn_df["txn_date"] <= dim_customer_slim["effective_end"])
            )
        ),
        how="left"
    ).drop(dim_customer_slim["customer_id"])

    #Step 7: Select final fact columns
    fact_df = txn_df.select(
        F.col("customer_key"),
        F.col("merchant_key"),
        F.col("payment_method_key"),
        F.col("date_key"),
        F.col("transaction_id"),
        F.col("txn_date"),
        F.col("txn_status"),
        F.col("failure_reason"),
        F.col("method_code"),
        F.col("txn_amount"),
        F.col("platform_fee"),
        F.col("cashback_amount"),
        F.col("net_revenue"),
        F.col("is_fraud_flag"),
        F.col("txn_year"),
        F.col("txn_month"),
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("created_at")
    )

    #Step 8: Data quality check
    total        = fact_df.count()
    null_customer = fact_df.filter(F.col("customer_key").isNull()).count()
    null_merchant = fact_df.filter(F.col("merchant_key").isNull()).count()
    null_date     = fact_df.filter(F.col("date_key").isNull()).count()

    logger.info(f"Total rows        : {total:,}")
    logger.info(f"NULL customer_key : {null_customer:,}")
    logger.info(f"NULL merchant_key : {null_merchant:,}")
    logger.info(f"NULL date_key     : {null_date:,}")

    #Step 9: Write to Gold
    (
        fact_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("txn_year", "txn_month")
        .save(f"{GOLD_PATH}/fact_transaction")
    )

    logger.success(f"✅ fact_transaction → {total:,} rows")

def main():
    logger.info("Starting Gold Layer — fact_transaction build")

    spark = get_spark(app_name="FactLoader")

    try:
        build_fact_transaction(spark)
        logger.success("✅ Gold Layer fact_transaction complete!")

    except Exception as e:
        logger.error(f"❌ Fact loader failed: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()