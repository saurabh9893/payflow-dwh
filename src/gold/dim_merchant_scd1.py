from src.utils.spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from delta.tables import DeltaTable
from loguru import logger
from datetime import datetime
import os

SILVER_PATH = "lakehouse/silver"
GOLD_PATH   = "lakehouse/gold"

#Hashing to make search easy 
def compute_hash(*cols):
    """
    Computes MD5 hash of multiple columns combined.
    Used to detect if a record has changed.
    
    Example:
      compute_hash("FOOD", "Mumbai", "MH", "1")
      → MD5("FOOD|Mumbai|MH|1")
      → "a3f8c2d1..."
    """
    combined = F.concat_ws("|", *[F.col(c).cast("string") for c in cols])
    return F.md5(combined)

#Logic of scd1 (No History is Maintained)
def load_dim_merchant(spark):
    logger.info("Loading dim_merchant — SCD Type 1")

    silver_df = spark.read.format("delta").load(f"{SILVER_PATH}/merchants")

    tracked_cols = [
        "merchant_name", "category", "sub_category",
        "city", "state", "is_active"
    ]

    silver_df = silver_df.withColumn(
        "record_hash",
        compute_hash(*tracked_cols)
    )

#Adding surrogate key before merge
    silver_df = silver_df.withColumn(
        "merchant_key",
        F.monotonically_increasing_id()+1
    )


    silver_df = silver_df.withColumn(
        "updated_at",
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    gold_path = f"{GOLD_PATH}/dim_merchant"

    if not os.path.exists(gold_path):
        logger.info("dim_merchant doesn't exist — doing initial load")


        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        logger.success(f"✅ dim_merchant initial load → {silver_df.count():,} rows")
        return

    logger.info("dim_merchant exists — running SCD1 MERGE")

    dim_merchant = DeltaTable.forPath(spark, gold_path)

    (
        dim_merchant.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.merchant_id = source.merchant_id"
        )
        # WHEN MATCHED AND hash changed → UPDATE
        .whenMatchedUpdate(
          condition = "target.record_hash != source.record_hash",
          set ={
            "target.merchant_name": "source.merchant_name",
            "target.category":      "source.category",
            "target.sub_category":  "source.sub_category",
            "target.city":          "source.city",
            "target.state":         "source.state",
            "target.is_active":     "source.is_active",
            "target.record_hash":   "source.record_hash",
            "target.updated_at":    "source.updated_at",
        })
        # WHEN NOT MATCHED → INSERT new merchant
        .whenNotMatchedInsert(
        values = {
            "merchant_id":   "source.merchant_id",
            "merchant_name": "source.merchant_name",
            "category":      "source.category",
            "sub_category":  "source.sub_category",
            "city":          "source.city",
            "state":         "source.state",
            "is_active":     "source.is_active",
            "record_hash":   "source.record_hash",
            "updated_at":    "source.updated_at",
            "merchant_key":  "source.merchant_key",
        })
        .execute()
    )

    final_count = spark.read.format("delta").load(gold_path).count()
    logger.success(f"✅ dim_merchant SCD1 MERGE complete → {final_count:,} rows")


#Scd1 for payment_method
def load_dim_payment_method(spark):
    logger.info("Loading dim_payment_method — SCD Type 1")

    silver_df = spark.read.format("delta").load(f"{SILVER_PATH}/payment_methods")

    tracked_cols = ["method_name", "method_type", "is_active"]

#Adding suroggate key before merge
    silver_df = (
        silver_df
        .withColumn("record_hash", compute_hash(*tracked_cols))
        .withColumn("updated_at",  F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        .withColumn("payment_method_key",F.monotonically_increasing_id()+1)
    )

    gold_path = f"{GOLD_PATH}/dim_payment_method"

    # First time load
    if not os.path.exists(gold_path):
        logger.info("dim_payment_method doesn't exist — doing initial load")


        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        logger.success(f"✅ dim_payment_method initial load → {silver_df.count():,} rows")
        return

    logger.info("dim_payment_method exists — running SCD1 MERGE")

    dim_payment_method = DeltaTable.forPath(spark, gold_path)

    (
        dim_payment_method.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.method_id = source.method_id"
        )
        .whenMatchedUpdate(condition = "target.record_hash != source.record_hash",
         set = {
            "method_name": "source.method_name",
            "method_type": "source.method_type",
            "is_active":   "source.is_active",
            "record_hash": "source.record_hash",
            "updated_at":  "source.updated_at",
        })
        .whenNotMatchedInsert(
        values = {
            "method_id":          "source.method_id",
            "method_code":        "source.method_code",
            "method_name":        "source.method_name",
            "method_type":        "source.method_type",
            "is_active":          "source.is_active",
            "record_hash":        "source.record_hash",
            "updated_at":         "source.updated_at",
            "payment_method_key": "source.payment_method_key",
        })
        .execute()
    )

    final_count = spark.read.format("delta").load(gold_path).count()
    logger.success(f"✅ dim_payment_method SCD1 MERGE complete → {final_count:,} rows")

def main():
    logger.info("Starting SCD Type 1 dimension loads")

    spark = get_spark(app_name="SCD1Loader")

    try:
        load_dim_merchant(spark)
        load_dim_payment_method(spark)

        logger.success("✅ SCD Type 1 loads complete!")

    except Exception as e:
        logger.error(f"❌ SCD1 load failed: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()