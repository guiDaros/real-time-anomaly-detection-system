from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    window,
    avg,
    count,
    sum,
    max,
    min,
    stddev
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

from core.config import settings


# =========================================
# SCHEMA
# =========================================

TRANSACTION_SCHEMA = StructType([
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


# =========================================
# SPARK SESSION
# =========================================

def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4"
        )
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation",
            "true"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(settings.LOG_LEVEL)

    return spark


# =========================================
# READ STREAM
# =========================================

def read_kafka_stream(spark: SparkSession):

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            settings.KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            settings.KAFKA_TOPIC_TRANSACTIONS
        )
        .option(
            "startingOffsets",
            "latest"
        )
        .option(
            "failOnDataLoss",
            "false"
        )
        .load()
    )


# =========================================
# PARSE EVENTS
# =========================================

def parse_transactions(df_raw):

    return (
        df_raw
        .select(
            from_json(
                col("value").cast("string"),
                TRANSACTION_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .filter(col("transaction_id").isNotNull())
    )


# =========================================
# EVENT TIME
# =========================================

def apply_event_time(df):

    return (
        df.withColumn(
            "event_time",
            to_timestamp(col("timestamp"))
        )
    )


# =========================================
# WATERMARK
# =========================================

def apply_watermark(df):

    return (
        df.withWatermark(
            "event_time",
            "10 minutes"
        )
    )


# =========================================
# FEATURE ENGINEERING
# =========================================

def build_features(df):

    return (
        df.groupBy(
            window(
                col("event_time"),
                "5 minutes"
            ),
            col("user_id")
        )
        .agg(
            count("*").alias("transaction_count_5min"),

            avg("amount").alias("avg_amount_5min"),

            sum("amount").alias("total_amount_5min"),

            max("amount").alias("max_amount_5min"),

            min("amount").alias("min_amount_5min"),

            stddev("amount").alias("std_amount_5min")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("user_id"),
            col("transaction_count_5min"),
            col("avg_amount_5min"),
            col("total_amount_5min"),
            col("max_amount_5min"),
            col("min_amount_5min"),
            col("std_amount_5min")
        )
    )


# =========================================
# OUTPUT STREAM
# =========================================

def write_console_stream(df):

    return (
        df.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .option(
            "checkpointLocation",
            settings.SPARK_CHECKPOINT_DIR
        )
        .trigger(
            processingTime=settings.SPARK_TRIGGER_INTERVAL
        )
        .start()
    )


# =========================================
# MAIN
# =========================================

def main():

    spark = create_spark_session()

    print("=" * 60)
    print("Spark Streaming started")
    print(f"App: {settings.SPARK_APP_NAME}")
    print(f"Kafka: {settings.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {settings.KAFKA_TOPIC_TRANSACTIONS}")
    print("=" * 60)

    df_raw = read_kafka_stream(spark)

    df_parsed = parse_transactions(df_raw)

    df_events = apply_event_time(df_parsed)

    df_watermarked = apply_watermark(df_events)

    df_features = build_features(df_watermarked)

    query = write_console_stream(df_features)

    query.awaitTermination()


if __name__ == "__main__":
    main()