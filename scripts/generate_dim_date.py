from src.utils.spark_session import get_spark
from pyspark.sql import functions as F

def generate_dim_date():
  spark = get_spark(app_name="GenerateDimDate")

  #step 1: generating dates using sequence(array format) & then explode
  df = spark.sql("""
                SELECT explode(sequence(
                       to_date("2018-01-01"),
                       to_date("2030-12-31"),
                       interval 1 day)) as full_date
                 """)
  
  #step 2: Creating dim_table
  df = (df.withColumn("date_key",F.date_format("full_date","yyyyMMdd").cast("int"))
        .withColumn("year",F.year("full_date"))
        .withColumn("quater",F.quarter("full_date"))
        .withColumn("month",F.month("full_date"))
        .withColumn("month_name",F.date_format("full_date","MMMM"))
        .withColumn("week_of_year",F.weekofyear("full_date"))
        .withColumn("day_of_week",F.dayofweek("full_date"))
        .withColumn("day_name",F.date_format("full_date","EEEE"))
        .withColumn("is_weekend",F.when(F.dayofweek("full_date").isin(1,7),1).otherwise(0))
        .withColumn("fiscal_year",F.when(F.month("full_date") >= 4,F.year("full_date")).otherwise(F.year("full_date") - 1))
        .withColumn("fiscal_quarter",F.when(F.month("full_date").isin(4, 5, 6), 1)
            .when(F.month("full_date").isin(7, 8, 9), 2)
            .when(F.month("full_date").isin(10, 11, 12), 3)
            .otherwise(4)))
  
         
      # Step 3: Write to Gold layer as Delta table

  (
    df.write
    .format("delta")
    .mode("overwrite")
    .save("lakehouse/gold/dim_date")
  )


  print(f"dim_date loaded successfully: {df.count()} rows")
  spark.stop()

if __name__ == "__main__":
    generate_dim_date()