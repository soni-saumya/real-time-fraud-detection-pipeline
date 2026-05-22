from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    when,
    hour,
    month,
    to_timestamp,
    trim
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("IndustryLevelSilverLayer") \
    .getOrCreate()

# Read Bronze Layer Data
transactions_df = spark.read \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\bronze\transactions_csv"
    )

print("Bronze data loaded successfully!")

# ---------------------------------------------------
# DATA TYPE CONVERSION
# ---------------------------------------------------

transactions_df = transactions_df.withColumn(
    "amount",
    col("amount").cast("double")
)

transactions_df = transactions_df.withColumn(
    "transaction_time",
    to_timestamp(col("transaction_time"))
)

# ---------------------------------------------------
# REMOVE NULL VALUES
# ---------------------------------------------------

cleaned_df = transactions_df.dropna()

# ---------------------------------------------------
# REMOVE DUPLICATE RECORDS
# ---------------------------------------------------

cleaned_df = cleaned_df.dropDuplicates()

# ---------------------------------------------------
# FILTER INVALID TRANSACTION AMOUNTS
# ---------------------------------------------------

cleaned_df = cleaned_df.filter(
    col("amount") > 0
)

# Remove unrealistic amounts
cleaned_df = cleaned_df.filter(
    col("amount") < 1000000
)

# ---------------------------------------------------
# VALIDATE CUSTOMER IDS
# ---------------------------------------------------

cleaned_df = cleaned_df.filter(
    col("customer_id").isNotNull()
)

# ---------------------------------------------------
# VALIDATE ACCOUNT IDS
# ---------------------------------------------------

cleaned_df = cleaned_df.filter(
    col("account_id").isNotNull()
)

# ---------------------------------------------------
# REMOVE EMPTY MERCHANT NAMES
# ---------------------------------------------------

cleaned_df = cleaned_df.filter(
    trim(col("merchant")) != ""
)

# ---------------------------------------------------
# STANDARDIZE TRANSACTION TYPE
# ---------------------------------------------------

cleaned_df = cleaned_df.withColumn(
    "transaction_type",
    upper(col("transaction_type"))
)


# ---------------------------------------------------
# FILTER INVALID COUNTRIES
# ---------------------------------------------------

valid_countries = [
    "INDIA",
    "USA",
    "UK",
    "CANADA",
    "GERMANY",
    "SINGAPORE"
]

cleaned_df = cleaned_df.filter(
    upper(col("country")).isin(valid_countries)
)

# ---------------------------------------------------
# HIGH AMOUNT FRAUD FLAG
# ---------------------------------------------------

cleaned_df = cleaned_df.withColumn(
    "high_amount_flag",
    when(col("amount") > 50000, "YES").otherwise("NO")
)

# ---------------------------------------------------
# NIGHT TRANSACTION FLAG
# ---------------------------------------------------

cleaned_df = cleaned_df.withColumn(
    "transaction_hour",
    hour(col("transaction_time"))
)

cleaned_df = cleaned_df.withColumn(
    "night_transaction_flag",
    when(
        (col("transaction_hour") >= 0) &
        (col("transaction_hour") <= 5),
        "YES"
    ).otherwise("NO")
)

# ---------------------------------------------------
# MONTH EXTRACTION
# ---------------------------------------------------

cleaned_df = cleaned_df.withColumn(
    "transaction_month",
    month(col("transaction_time"))
)

# ---------------------------------------------------
# SUSPICIOUS COUNTRY FLAG
# ---------------------------------------------------

cleaned_df = cleaned_df.withColumn(
    "international_transaction_flag",
    when(
        upper(col("country")) != "INDIA",
        "YES"
    ).otherwise("NO")
)

# ---------------------------------------------------
# SHOW RESULTS
# ---------------------------------------------------

print("Total Records After Cleaning:", cleaned_df.count())

cleaned_df.show(20)

cleaned_df.printSchema()

# ---------------------------------------------------
# WRITE TO SILVER LAYER
# ---------------------------------------------------

cleaned_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\my_projects\fraud_project\silver\clean_transactions"
    )

print("Industry-level Silver Layer created successfully!")

# Stop Spark
spark.stop()