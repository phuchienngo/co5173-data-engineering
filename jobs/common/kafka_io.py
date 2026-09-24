import json
import os

from kafka import KafkaProducer


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
