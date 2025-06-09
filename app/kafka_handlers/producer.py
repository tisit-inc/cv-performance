import json
import asyncio
from datetime import datetime

from confluent_kafka import Producer
from app.config.config import get_settings
from app.schemas.inference import InferenceOutput
from app.utils.logger import setup_logger


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


class KafkaProducer:
    def __init__(self):
        self.settings = get_settings()
        self.logger = setup_logger("kafka_producer")
        self.producer = Producer(
            {"bootstrap.servers": self.settings.kafka_bootstrap_servers}
        )
        self.flush_interval = 1.0  # Flush каждую секунду

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    async def send_message(self, result: InferenceOutput):
        try:
            message = json.dumps(result.model_dump(), default=json_serializer).encode(
                "utf-8"
            )
            self.producer.produce(
                self.settings.kafka_output_topic, message, callback=self.delivery_report
            )
            self.logger.info(f"Message queued for Kafka: {result.frame_track_uuid}")
        except BufferError:
            self.logger.warning("Local producer queue is full, waiting before retrying")
            await asyncio.sleep(1)
            await self.send_message(result)  # Retry
        except Exception as e:
            self.logger.error(f"Error sending message to Kafka: {e}")

    async def run(self):
        while True:
            await asyncio.sleep(self.flush_interval)
            await asyncio.to_thread(self.producer.flush)

    async def close(self):
        self.logger.info("Closing Kafka producer")
        await asyncio.to_thread(self.producer.flush)
