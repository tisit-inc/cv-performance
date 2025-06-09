# tests/test_integration.py
import pytest
import json
import asyncio
from datetime import datetime
from app.core.config import get_settings
from app.services.redis_buffer import RedisBuffer
from app.kafka_handlers.consumer import KafkaConsumerService


@pytest.mark.asyncio
async def test_kafka_consumer_to_redis(docker_compose):
    """
    Этот тест предполагает, что Kafka и Redis запущены через Docker Compose.
    """
    settings = get_settings()
    redis_buffer = RedisBuffer(redis_url=settings.redis_url, environment="test")
    await redis_buffer.connect()

    kafka_consumer = KafkaConsumerService(redis_buffer=redis_buffer)

    # Запускаем consumer в фоновом режиме
    consumer_task = asyncio.create_task(kafka_consumer.run())

    # Отправляем тестовое сообщение в Kafka
    test_message = {
        "frame_track_uuid": "test123",
        "timestamp": datetime.utcnow().isoformat(),
        "exercise": "pushup",
        "session_uuid": "test_session",
        "landmarks": [{"x": 10, "y": 20, "z": 30}],
        "metrics": {"angle": 45}
    }

    # Используем asyncio.to_thread для отправки сообщения
    async def produce_message():
        from confluent_kafka import Producer
        producer = Producer({'bootstrap.servers': settings.kafka_bootstrap_servers})
        producer.produce(settings.kafka_input_topic, json.dumps(test_message).encode('utf-8'))
        producer.flush()

    await produce_message()

    # Дадим время для обработки сообщения
    await asyncio.sleep(2)

    # Проверим, что сообщение сохранено в Redis
    frames = await redis_buffer.get_frames(
        "test_session",
        start_time=(datetime.utcnow() - timedelta(seconds=10)).timestamp(),
        end_time=datetime.utcnow().timestamp()
    )

    assert len(frames) > 0
    assert frames[0]['frame_track_uuid'] == "test123"

    # Остановим consumer
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    await redis_buffer.disconnect()
