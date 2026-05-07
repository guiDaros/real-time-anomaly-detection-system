from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def main():
    spark = (
        SparkSession.builder
        .appName("AnomalyDetectionStreaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("amount", DoubleType()),
        StructField("merchant", StringType()),
        StructField("category", StringType()),
        StructField("device_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ])

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions_raw")
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = (
        df_parsed.writeStream
        .format("console")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()