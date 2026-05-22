from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mysql.connector
# ---------------------------------------------------
# CREATE SPARK SESSION
# ---------------------------------------------------

spark = SparkSession.builder \
    .appName("RealTimeFraudDetection") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# ---------------------------------------------------
# MYSQL CONNECTION
# ---------------------------------------------------

connection = mysql.connector.connect(

    host="localhost",
    user="root",
    password="1234",
    database="fraud_detection_db"
)

cursor = connection.cursor()

# ---------------------------------------------------
# KAFKA STREAM SCHEMA
# ---------------------------------------------------

schema = StructType([

    StructField("transaction_id", IntegerType()),

    StructField("customer_id", IntegerType()),

    StructField("account_id", IntegerType()),

    StructField("amount", DoubleType()),

    StructField("merchant", StringType()),

    StructField("transaction_type", StringType()),

    StructField("country", StringType()),

    StructField("device_id", StringType()),

    StructField("transaction_time", StringType())
])

# ---------------------------------------------------
# READ STREAM FROM KAFKA
# ---------------------------------------------------

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fraud_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# ---------------------------------------------------
# CONVERT JSON DATA
# ---------------------------------------------------

json_df = df.selectExpr(
    "CAST(value AS STRING)"
)

parsed_df = json_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
).select("data.*")

# ---------------------------------------------------
# FRAUD DETECTION RULES
# ---------------------------------------------------

fraud_df = parsed_df.withColumn(

    "fraud_flag",

    when(col("amount") > 100000, "HIGH_AMOUNT")

    .when(col("country") != "India", "INTERNATIONAL")

    .otherwise("NORMAL")
)
# ---------------------------------------------------
# FUNCTION TO SAVE FRAUD ALERTS
# ---------------------------------------------------

def save_to_mysql(batch_df, batch_id):

    rows = batch_df.collect()

    for row in rows:

        query = """

        INSERT INTO real_time_fraud_alerts (

            transaction_id,
            customer_id,
            account_id,
            amount,
            merchant,
            transaction_type,
            country,
            device_id,
            transaction_time,
            fraud_flag

        )

        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

        """

        values = (

            row.transaction_id,
            row.customer_id,
            row.account_id,
            row.amount,
            row.merchant,
            row.transaction_type,
            row.country,
            row.device_id,
            row.transaction_time,
            row.fraud_flag
        )

        cursor.execute(query, values)

    connection.commit()

    print(f"Batch {batch_id} stored successfully!")

# ---------------------------------------------------
# FILTER SUSPICIOUS TRANSACTIONS
# ---------------------------------------------------

suspicious_df = fraud_df.filter(
    col("fraud_flag") != "NORMAL"
)

# ---------------------------------------------------
# OUTPUT LIVE ALERTS
# ---------------------------------------------------

query = suspicious_df.writeStream \
    .foreachBatch(save_to_mysql) \
    .outputMode("append") \
    .start()

query.awaitTermination()