from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeLayerPipeline") \
    .config(
        "spark.jars",
        r"C:\Users\adity\OneDrive\Desktop\fraud_project\jars\mysql-connector-j-9.7.0\mysql-connector-j-9.7.0.jar"
    ) \
    .getOrCreate()

# Read transactions table
transactions_df = spark.read.format("jdbc") \
    .option(
        "url",
        "jdbc:mysql://localhost:3306/fraud_detection_db"
    ) \
    .option(
        "driver",
        "com.mysql.cj.jdbc.Driver"
    ) \
    .option(
        "dbtable",
        "transactions"
    ) \
    .option(
        "user",
        "root"
    ) \
    .option(
        "password",
        "1234"
    ) \
    .load()
print("Data loaded from MySQL")

print("Total Records:", transactions_df.count())

# Write raw data into bronze layer
transactions_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\spark_data\bronze\transactions_csv"
    )
print("Bronze layer created successfully!")

spark.stop()