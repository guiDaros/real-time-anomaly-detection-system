from dataclasses import dataclass
from dotenv import load_dotenv
import os


load_dotenv()


@dataclass(frozen=True)
class Settings:

    # =====================================
    # PROJECT
    # =====================================

    PROJECT_NAME: str = os.getenv(
        "PROJECT_NAME",
        "anomaly-detection-system"
    )

    ENVIRONMENT: str = os.getenv(
        "ENVIRONMENT",
        "development"
    )

    # =====================================
    # KAFKA
    # =====================================

    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )

    KAFKA_TOPIC_TRANSACTIONS: str = os.getenv(
        "KAFKA_TOPIC_TRANSACTIONS",
        "transactions"
    )

    KAFKA_TOPIC_RISK_SCORES: str = os.getenv(
        "KAFKA_TOPIC_RISK_SCORES",
        "risk_scores"
    )

    # =====================================
    # PRODUCER
    # =====================================

    PRODUCER_EVENTS_PER_SECOND: int = int(
        os.getenv("PRODUCER_EVENTS_PER_SECOND", 10)
    )

    PRODUCER_ANOMALY_RATE: float = float(
        os.getenv("PRODUCER_ANOMALY_RATE", 0.03)
    )

    PRODUCER_SLEEP_SECONDS: float = float(
        os.getenv("PRODUCER_SLEEP_SECONDS", 0.1)
    )

    # =====================================
    # SPARK
    # =====================================

    SPARK_APP_NAME: str = os.getenv(
        "SPARK_APP_NAME",
        "AnomalyDetectionStreaming"
    )

    SPARK_MASTER: str = os.getenv(
        "SPARK_MASTER",
        "local[*]"
    )

    SPARK_CHECKPOINT_DIR: str = os.getenv(
        "SPARK_CHECKPOINT_DIR",
        "checkpoints/"
    )

    SPARK_TRIGGER_INTERVAL: str = os.getenv(
        "SPARK_TRIGGER_INTERVAL",
        "5 seconds"
    )

    # =====================================
    # REDIS
    # =====================================

    REDIS_HOST: str = os.getenv(
        "REDIS_HOST",
        "localhost"
    )

    REDIS_PORT: int = int(
        os.getenv("REDIS_PORT", 6379)
    )

    # =====================================
    # MINIO
    # =====================================

    MINIO_ENDPOINT: str = os.getenv(
        "MINIO_ENDPOINT",
        "localhost:9000"
    )

    MINIO_ACCESS_KEY: str = os.getenv(
        "MINIO_ACCESS_KEY",
        "admin"
    )

    MINIO_SECRET_KEY: str = os.getenv(
        "MINIO_SECRET_KEY",
        "password123"
    )

    MINIO_BUCKET: str = os.getenv(
        "MINIO_BUCKET",
        "anomaly-detection"
    )

    MINIO_SECURE: bool = os.getenv(
        "MINIO_SECURE",
        "false"
    ).lower() == "true"

    # =====================================
    # STORAGE
    # =====================================

    DATA_LAKE_ROOT: str = os.getenv(
        "DATA_LAKE_ROOT",
        "data-lake/"
    )

    BRONZE_PATH: str = os.getenv(
        "BRONZE_PATH",
        "bronze/"
    )

    SILVER_PATH: str = os.getenv(
        "SILVER_PATH",
        "silver/"
    )

    GOLD_PATH: str = os.getenv(
        "GOLD_PATH",
        "gold/"
    )

    # =====================================
    # LOGGING
    # =====================================

    LOG_LEVEL: str = os.getenv(
        "LOG_LEVEL",
        "INFO"
    )


settings = Settings()