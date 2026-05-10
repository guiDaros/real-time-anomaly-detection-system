import json
import logging
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings


logger = logging.getLogger(__name__)


class TransactionKafkaProducer:

    def __init__(self):

        self.producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            acks="all",
            linger_ms=10,
            batch_size=16384,
            max_in_flight_requests_per_connection=5,
            request_timeout_ms=30000,
        )

    def send_transaction(self, transaction: Dict[str, Any]) -> None:

        try:

            future = self.producer.send(
                settings.KAFKA_TOPIC_TRANSACTIONS,
                value=transaction
            )

            metadata = future.get(timeout=10)

            logger.info(
                "Transaction sent | topic=%s partition=%s offset=%s transaction_id=%s",
                metadata.topic,
                metadata.partition,
                metadata.offset,
                transaction["transaction_id"]
            )

        except KafkaError as e:

            logger.exception(
                "Kafka error while sending transaction: %s",
                str(e)
            )

    def flush(self):

        self.producer.flush()

    def close(self):

        self.producer.flush()
        self.producer.close()