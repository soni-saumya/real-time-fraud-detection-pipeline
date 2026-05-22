from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    min,
    when,
    hour,
    round
)

# ---------------------------------------------------
# CREATE SPARK SESSION
# ---------------------------------------------------

spark = SparkSession.builder \
    .appName("EnterpriseGoldLayer") \
    .getOrCreate()

# ---------------------------------------------------
# READ SILVER LAYER
# ---------------------------------------------------

df = spark.read \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\silver\clean_transactions",
        inferSchema=True
    )

print("Silver layer loaded successfully!")

# ---------------------------------------------------
# CUSTOMER RISK PROFILE
# ---------------------------------------------------

customer_risk_df = df.groupBy("customer_id").agg(
    count("*").alias("total_transactions"),

    round(avg("amount"), 2).alias("avg_transaction_amount"),

    max("amount").alias("max_transaction_amount"),

    min("amount").alias("min_transaction_amount"),

    sum(
        when(col("high_amount_flag") == "YES", 1).otherwise(0)
    ).alias("high_amount_transactions"),

    sum(
        when(col("night_transaction_flag") == "YES", 1).otherwise(0)
    ).alias("night_transactions"),

    sum(
        when(
            col("international_transaction_flag") == "YES",
            1
        ).otherwise(0)
    ).alias("international_transactions")
)

# Risk Score Calculation
customer_risk_df = customer_risk_df.withColumn(
    "risk_score",

    (
        col("high_amount_transactions") * 30 +
        col("night_transactions") * 20 +
        col("international_transactions") * 40
    )
)

# Risk Category
customer_risk_df = customer_risk_df.withColumn(
    "risk_category",

    when(col("risk_score") >= 100, "HIGH")
    .when(col("risk_score") >= 50, "MEDIUM")
    .otherwise("LOW")
)

# ---------------------------------------------------
# HIGH RISK TRANSACTIONS
# ---------------------------------------------------

high_risk_transactions_df = df.filter(
    (col("high_amount_flag") == "YES") |
    (col("night_transaction_flag") == "YES") |
    (col("international_transaction_flag") == "YES")
)

# ---------------------------------------------------
# MERCHANT RISK ANALYSIS
# ---------------------------------------------------

merchant_risk_df = df.groupBy("merchant").agg(
    count("*").alias("total_transactions"),

    round(avg("amount"), 2).alias("avg_transaction_amount"),

    sum(
        when(col("high_amount_flag") == "YES", 1).otherwise(0)
    ).alias("high_risk_transactions")
)

# ---------------------------------------------------
# DEVICE FRAUD ANALYSIS
# ---------------------------------------------------

device_risk_df = df.groupBy("device_id").agg(
    count("*").alias("device_transaction_count"),

    sum(
        when(col("night_transaction_flag") == "YES", 1).otherwise(0)
    ).alias("night_transactions"),

    sum(
        when(
            col("international_transaction_flag") == "YES",
            1
        ).otherwise(0)
    ).alias("international_transactions")
)

# ---------------------------------------------------
# COUNTRY RISK ANALYSIS
# ---------------------------------------------------

country_risk_df = df.groupBy("country").agg(
    count("*").alias("total_transactions"),

    round(avg("amount"), 2).alias("avg_transaction_amount"),

    sum(
        when(col("high_amount_flag") == "YES", 1).otherwise(0)
    ).alias("high_risk_transactions")
)

# ---------------------------------------------------
# HOURLY TRANSACTION MONITORING
# ---------------------------------------------------

hourly_monitoring_df = df.groupBy("transaction_hour").agg(
    count("*").alias("total_transactions"),

    round(avg("amount"), 2).alias("avg_transaction_amount")
)

# ---------------------------------------------------
# AML MONITORING
# ---------------------------------------------------

aml_df = df.filter(
    col("amount") > 100000
)

# ---------------------------------------------------
# EXECUTIVE KPI DASHBOARD
# ---------------------------------------------------

executive_kpi_df = df.agg(
    count("*").alias("total_transactions"),

    round(avg("amount"), 2).alias("average_transaction_amount"),

    max("amount").alias("highest_transaction"),

    min("amount").alias("lowest_transaction")
)

# ---------------------------------------------------
# SUSPICIOUS ACTIVITY REPORT
# ---------------------------------------------------

sar_df = df.filter(
    (col("high_amount_flag") == "YES") &
    (col("international_transaction_flag") == "YES")
)

# ---------------------------------------------------
# WRITE GOLD TABLES
# ---------------------------------------------------

customer_risk_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\customer_risk_profile"
    )

high_risk_transactions_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\high_risk_transactions"
    )

merchant_risk_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\merchant_risk_analysis"
    )

device_risk_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\device_risk_analysis"
    )

country_risk_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\country_risk_analysis"
    )

hourly_monitoring_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\hourly_transaction_monitoring"
    )

aml_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\aml_monitoring"
    )

executive_kpi_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\executive_kpis"
    )

sar_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\gold\suspicious_activity_report"
    )

print("Enterprise Gold Layer created successfully!")

# ---------------------------------------------------
# STOP SPARK
# ---------------------------------------------------

spark.stop()