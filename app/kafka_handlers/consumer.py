# app/kafka_handlers/consumer.py
import json
import asyncio
from confluent_kafka import Consumer, KafkaError
from app.core.config import get_settings
from app.schemas.performance import PerformanceInput
from app.utils.logger import setup_logger


class KafkaConsumer:
    def __init__(self):
        self.settings = get_settings()
        self.logger = setup_logger("kafka_consumer")
        self.logger.info("Initializing Kafka Consumer")
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "group.id": self.settings.kafka_group_id,
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe([self.settings.kafka_input_topic])
        self.logger.info(f"Subscribed to topic: {self.settings.kafka_input_topic}")

    async def receive_message(self) -> PerformanceInput | None:
        try:
            self.logger.debug("Polling for message...")
            msg = await asyncio.to_thread(self.consumer.poll, 1.0)
            if msg is None:
                self.logger.debug("No message received")
                return None
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.info("Reached end of partition")
                else:
                    self.logger.error(f"Consumer error: {msg.error()}")
                return None

            data = json.loads(msg.value().decode("utf-8"))
            self.logger.info(f"Received message: {data['frame_track_uuid']}")
            return PerformanceInput(**data)

        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error: {e}")
        except Exception as e:
            self.logger.error(
                f"Unexpected error in receive_message: {e}", exc_info=True
            )
        return None

    async def run(self):
        self.logger.info("Starting Kafka Consumer run loop")
        while True:
            message = await self.receive_message()
            if message:
                yield message
            else:
                await asyncio.sleep(0.1)  # a small pause to not load CPU

    async def close(self):
        self.logger.info("Closing Kafka consumer")
        await asyncio.to_thread(self.consumer.close)
