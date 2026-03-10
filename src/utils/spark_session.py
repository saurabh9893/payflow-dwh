from pyspark.sql import SparkSession
import os
import sys
from pathlib import Path

# Fix Python path for Spark workers
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Detect if running inside Docker or locally
JARS_DIR = "/opt/airflow/jars" if os.path.exists("/opt/airflow/jars") \
           else str(Path.home() / ".ivy2/jars")

DELTA_JARS = ",".join([
    f"{JARS_DIR}/io.delta_delta-spark_2.12-3.1.0.jar",
    f"{JARS_DIR}/io.delta_delta-storage-3.1.0.jar",
    f"{JARS_DIR}/org.antlr_antlr4-runtime-4.9.3.jar",
])

def get_spark(app_name: str = "PayFlowDWH") -> SparkSession:

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", DELTA_JARS)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
