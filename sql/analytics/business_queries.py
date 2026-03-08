from src.utils.spark_session import get_spark
from loguru import logger

GOLD_PATH = "lakehouse/gold"

def register_views(spark):
    """Register all Gold tables as Spark SQL temp views."""
    
    logger.info("Registering Gold tables as SQL views")

    spark.read.format("delta").load(f"{GOLD_PATH}/fact_transaction") \
         .createOrReplaceTempView("fact_transaction")

    spark.read.format("delta").load(f"{GOLD_PATH}/dim_customer") \
         .createOrReplaceTempView("dim_customer")

    spark.read.format("delta").load(f"{GOLD_PATH}/dim_merchant") \
         .createOrReplaceTempView("dim_merchant")

    spark.read.format("delta").load(f"{GOLD_PATH}/dim_payment_method") \
         .createOrReplaceTempView("dim_payment_method")

    spark.read.format("delta").load(f"{GOLD_PATH}/dim_date") \
         .createOrReplaceTempView("dim_date")

    logger.success("✅ All views registered")

def q1_monthly_gmv(spark):
    logger.info("Q1: Monthly GMV by year/month")

    result = spark.sql("""
        SELECT
            d.year,
            d.month_name,
            d.month,
            COUNT(f.transaction_id)         AS total_transactions,
            SUM(f.txn_amount)               AS gross_merchandise_value,
            SUM(f.platform_fee)             AS total_platform_revenue,
            SUM(f.cashback_amount)          AS total_cashback,
            SUM(f.net_revenue)              AS net_revenue,
            ROUND(AVG(f.txn_amount), 2)     AS avg_transaction_value
        FROM fact_transaction f
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE f.txn_status = 'SUCCESS'
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """)

    print("\n" + "="*60)
    print("Q1: Monthly GMV (SUCCESS transactions only)")
    print("="*60)
    result.show(24, truncate=False)
    return result

def q2_top_merchants(spark):
    logger.info("Q2: Top 10 merchants by revenue")

    result = spark.sql("""
        SELECT
            m.merchant_name,
            m.category,
            m.city,
            COUNT(f.transaction_id)          AS total_transactions,
            SUM(f.txn_amount)                AS total_gmv,
            SUM(f.platform_fee)              AS total_revenue,
            ROUND(AVG(f.txn_amount), 2)      AS avg_txn_value,
            SUM(CASE WHEN f.txn_status = 'FAILED' 
                THEN 1 ELSE 0 END)           AS failed_transactions,
            ROUND(SUM(CASE WHEN f.txn_status = 'FAILED'
                THEN 1 ELSE 0 END) * 100.0 
                / COUNT(*), 2)               AS failure_rate_pct
        FROM fact_transaction f
        JOIN dim_merchant m ON f.merchant_key = m.merchant_key
        GROUP BY m.merchant_name, m.category, m.city
        ORDER BY total_revenue DESC
        LIMIT 10
    """)

    print("\n" + "="*60)
    print("Q2: Top 10 Merchants by Platform Revenue")
    print("="*60)
    result.show(10, truncate=False)
    return result

def q3_payment_method_analysis(spark):
    logger.info("Q3: Transaction success rate by payment method")

    result = spark.sql("""
        SELECT
            p.method_name,
            p.method_type,
            COUNT(f.transaction_id)             AS total_transactions,
            SUM(CASE WHEN f.txn_status = 'SUCCESS'
                THEN 1 ELSE 0 END)              AS successful_txns,
            SUM(CASE WHEN f.txn_status = 'FAILED'
                THEN 1 ELSE 0 END)              AS failed_txns,
            ROUND(SUM(CASE WHEN f.txn_status = 'SUCCESS'
                THEN 1 ELSE 0 END) * 100.0
                / COUNT(*), 2)                  AS success_rate_pct,
            ROUND(AVG(f.txn_amount), 2)         AS avg_txn_amount,
            SUM(f.platform_fee)                 AS total_revenue
        FROM fact_transaction f
        JOIN dim_payment_method p
            ON f.payment_method_key = p.payment_method_key
        GROUP BY p.method_name, p.method_type
        ORDER BY total_transactions DESC
    """)

    print("\n" + "="*60)
    print("Q3: Payment Method Success Rate Analysis")
    print("="*60)
    result.show(truncate=False)
    return result

def q4_customer_segment_revenue(spark):
    logger.info("Q4: Revenue breakdown by customer segment")

    result = spark.sql("""
        SELECT
            c.customer_segment,
            c.kyc_tier,
            COUNT(DISTINCT f.customer_key)      AS unique_customers,
            COUNT(f.transaction_id)             AS total_transactions,
            SUM(f.txn_amount)                   AS total_gmv,
            SUM(f.platform_fee)                 AS total_revenue,
            ROUND(AVG(f.txn_amount), 2)         AS avg_txn_value,
            ROUND(SUM(f.txn_amount) / 
                COUNT(DISTINCT f.customer_key), 
                2)                              AS gmv_per_customer,
            SUM(f.is_fraud_flag)                AS fraud_transactions,
            ROUND(SUM(f.is_fraud_flag) * 100.0
                / COUNT(*), 2)                  AS fraud_rate_pct
        FROM fact_transaction f
        JOIN dim_customer c
            ON f.customer_key = c.customer_key
        WHERE c.is_current = 1
        GROUP BY c.customer_segment, c.kyc_tier
        ORDER BY total_revenue DESC
    """)

    print("\n" + "="*60)
    print("Q4: Customer Segment Revenue Breakdown")
    print("="*60)
    result.show(truncate=False)
    return result

def q5_point_in_time_kyc(spark):
    logger.info("Q5: Revenue by KYC tier AT TIME of transaction")

    result = spark.sql("""
        SELECT
            c.kyc_tier                          AS kyc_tier_at_time_of_txn,
            COUNT(f.transaction_id)             AS total_transactions,
            COUNT(DISTINCT c.customer_id)       AS unique_customers,
            SUM(f.txn_amount)                   AS total_gmv,
            SUM(f.platform_fee)                 AS total_revenue,
            ROUND(AVG(f.txn_amount), 2)         AS avg_txn_value
        FROM fact_transaction f
        JOIN dim_customer c
            ON f.customer_key = c.customer_key
        GROUP BY c.kyc_tier
        ORDER BY total_revenue DESC
    """)

    print("\n" + "="*60)
    print("Q5: Revenue by KYC Tier AT TIME of Transaction")
    print("(Point-in-time — not current KYC tier)")
    print("="*60)
    result.show(truncate=False)

    # ── Bonus: Show difference between current vs historical ──
    comparison = spark.sql("""
        SELECT
            c.kyc_tier                      AS kyc_tier_at_txn_time,
            c.is_current,
            COUNT(f.transaction_id)         AS transactions,
            SUM(f.txn_amount)               AS gmv
        FROM fact_transaction f
        JOIN dim_customer c
            ON f.customer_key = c.customer_key
        GROUP BY c.kyc_tier, c.is_current
        ORDER BY c.kyc_tier, c.is_current
    """)

    print("\n── Breakdown: Current vs Historical versions ──")
    comparison.show(truncate=False)
    return result

def main():
    logger.info("Starting analytical queries on Gold layer")

    spark = get_spark(app_name="Analytics")

    try:
        register_views(spark)

        q1_monthly_gmv(spark)
        q2_top_merchants(spark)
        q3_payment_method_analysis(spark)
        q4_customer_segment_revenue(spark)
        q5_point_in_time_kyc(spark)

        logger.success("✅ All analytical queries complete!")

    except Exception as e:
        logger.error(f"❌ Analytics failed: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()