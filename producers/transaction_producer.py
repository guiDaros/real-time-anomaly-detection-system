import json
import random
import time
from datetime import datetime
from uuid import uuid4

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


MERCHANTS = [
    "Amazon",
    "Uber",
    "iFood",
    "Steam",
    "Netflix",
    "Airbnb",
]

CATEGORIES = [
    "shopping",
    "transport",
    "food",
    "gaming",
    "streaming",
    "travel",
]


def generate_transaction():
    return {
        "transaction_id": str(uuid4()),
        "user_id": random.randint(1, 100),
        "timestamp": datetime.utcnow().isoformat(),
        "amount": round(random.uniform(5, 5000), 2),
        "merchant": random.choice(MERCHANTS),
        "category": random.choice(CATEGORIES),
        "device_id": f"device_{random.randint(1, 20)}",
        "latitude": round(random.uniform(-23.7, -23.4), 6),
        "longitude": round(random.uniform(-46.8, -46.3), 6),
    }


def main():
    while True:
        transaction = generate_transaction()

        producer.send(
            "transactions_raw",
            value=transaction,
        )

        print(transaction)

        time.sleep(1)


if __name__ == "__main__":
    main()