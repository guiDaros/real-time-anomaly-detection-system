from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("AnomalyDetectionStreaming")
        .master("local[*]")
        .getOrCreate()
    )

    print("Spark version:", spark.version)

    spark.stop()


if __name__ == "__main__":
    main()