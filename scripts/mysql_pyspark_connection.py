
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FraudDetectionPipeline") \
    .config(
        "spark.jars",
        r"C:\Users\adity\OneDrive\Desktop\fraud_project\jars\mysql-connector-j-9.7.0\mysql-connector-j-9.7.0.jar"
    ) \
    .getOrCreate()

df = spark.read.format("jdbc") \
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

df.show(10)

df.printSchema()

spark.stop()