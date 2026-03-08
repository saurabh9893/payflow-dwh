from src.utils.spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from delta.tables import DeltaTable
from loguru import logger
from datetime import datetime, date
import os

SILVER_PATH = "lakehouse/silver"
GOLD_PATH   = "lakehouse/gold"

TRACKED_COLS = [
    "kyc_tier",
    "wallet_limit",
    "customer_segment",
    "city",
    "state"
]

def compute_hash(df, cols):
    """Compute MD5 hash of tracked columns for change detection."""
    combined = F.concat_ws("|", *[F.col(c).cast("string") for c in cols])
    return df.withColumn("record_hash", F.md5(combined))


def initial_load(spark, silver_df):
    """First time load — dim_customer doesn't exist yet."""
    logger.info("dim_customer doesn't exist — doing initial load")

    gold_path = f"{GOLD_PATH}/dim_customer"

    today = date.today().strftime("%Y-%m-%d")

    silver_df = (
        silver_df
        .withColumn("customer_key",    F.monotonically_increasing_id() + 1)
        .withColumn("effective_start", F.lit(today).cast(DateType()))
        .withColumn("effective_end",   F.lit(None).cast(DateType()))
        .withColumn("is_current",      F.lit(1).cast(IntegerType()))
    )

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .save(gold_path)
    )

    logger.success(f"✅ dim_customer initial load → {silver_df.count():,} rows")

def scd2_merge(spark, silver_df):
    """
    SCD Type 2 merge logic.
    
    For changed customers:
      → Expire old row (is_current=0, effective_end=today-1)
      → Insert new row (is_current=1, effective_end=NULL)
    
    For new customers:
      → Insert with is_current=1
    
    For unchanged customers:
      → Do nothing
    """
    logger.info("dim_customer exists — running SCD2 merge")

    gold_path = f"{GOLD_PATH}/dim_customer"
    today     = date.today().strftime("%Y-%m-%d")
    yesterday = (date.today() - __import__('datetime').timedelta(days=1)).strftime("%Y-%m-%d")

    #Step 1: Read existing CURRENT rows from Gold
    dim_customer = DeltaTable.forPath(spark, gold_path)
    current_df   = (
        spark.read
        .format("delta")
        .load(gold_path)
        .filter(F.col("is_current") == 1)
    )

    #Step 2: Join Silver to current Gold rows
    # Find out what changed
    joined_df = silver_df.alias("source").join(
        current_df.select(
            "customer_id",
            "record_hash",
            "customer_key"
        ).alias("target"),
        on="customer_id",
        how="left"  # left join keeps new customers too
    )

    #Step 3: Identify changed customers
    # Hash exists in target BUT is different from source
    changed_df = joined_df.filter(
        (F.col("target.record_hash").isNotNull()) &
        (F.col("source.record_hash") != F.col("target.record_hash"))
    ).select("source.*")

    logger.info(f"Changed customers found: {changed_df.count()}")

    #Step 4: Identify new customers
    # No matching row in target at all
    new_df = joined_df.filter(
        F.col("target.record_hash").isNull()
    ).select("source.*")

    new_df = new_df.cache()

    logger.info(f"New customers found: {new_df.count()}")

    #Step 5: Expire changed rows in Gold
    if changed_df.count() > 0:
        changed_ids = [
            row["customer_id"]
            for row in changed_df.select("customer_id").collect()
        ]

        changed_df = changed_df.cache()

        (
            dim_customer.update(
                condition=F.col("customer_id").isin(changed_ids) &
                          (F.col("is_current") == 1),
                set={
                    "is_current":    F.lit(0),
                    "effective_end": F.lit(yesterday).cast(DateType()),
                }
            )
        )
        logger.info(f"Expired {len(changed_ids)} old customer rows")

    #Step 6: Insert new versions + new customers

    changed_count = changed_df.count()
    new_count = new_df.count()
    rows_to_insert = changed_df.union(new_df)

    if changed_count + new_count > 0:
        rows_to_insert = (
            rows_to_insert
            .withColumn("customer_key",    F.monotonically_increasing_id() + 99999)
            .withColumn("effective_start", F.lit(today).cast(DateType()))
            .withColumn("effective_end",   F.lit(None).cast(DateType()))
            .withColumn("is_current",      F.lit(1).cast(IntegerType()))
        )

        (
            rows_to_insert.write
            .format("delta")
            .mode("append")
            .save(gold_path)
        )

        logger.info(f"Inserted {changed_count + new_count} new/updated rows")

    #Step 7: Final count
    final_df    = spark.read.format("delta").load(gold_path)
    total       = final_df.count()
    current     = final_df.filter(F.col("is_current") == 1).count()
    historical  = final_df.filter(F.col("is_current") == 0).count()

    logger.success(f"✅ dim_customer SCD2 complete")
    logger.info(f"   Total rows      : {total:,}")
    logger.info(f"   Current rows    : {current:,}")
    logger.info(f"   Historical rows : {historical:,}")


def main():
    logger.info("Starting SCD Type 2 — dim_customer")

    spark = get_spark(app_name="SCD2Customer")

    try:
        gold_path = f"{GOLD_PATH}/dim_customer"

        silver_df = spark.read.format("delta").load(f"{SILVER_PATH}/customers")

        silver_df = compute_hash(silver_df, TRACKED_COLS)

        if not os.path.exists(gold_path):
            initial_load(spark, silver_df)
        else:
            scd2_merge(spark, silver_df)

        logger.success("✅ dim_customer SCD2 complete!")

    except Exception as e:
        logger.error(f"❌ dim_customer SCD2 failed: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()