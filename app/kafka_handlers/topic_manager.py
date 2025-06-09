from confluent_kafka.admin import AdminClient, NewTopic
from app.core.config import get_settings
from app.utils.logger import setup_logger

class TopicManager:
    def __init__(self):
        self.settings = get_settings()
        self.admin_client = AdminClient(
            {"bootstrap.servers": self.settings.kafka_bootstrap_servers}
        )
        self.logger = setup_logger("topic_manager")

    def create_topics(self, topics):
        new_topics = [
            NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics
        ]
        fs = self.admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                self.logger.info(f"Topic {topic} created")
            except Exception as e:
                self.logger.error(f"Failed to create topic {topic}: {e}")

    def ensure_topics_exist(self):
        topics_to_create = [
            self.settings.kafka_input_topic,  # inference_results
            self.settings.kafka_output_topic,  # performance_feedback
        ]
        existing_topics = self.admin_client.list_topics().topics
        topics_to_create = [
            topic for topic in topics_to_create if topic not in existing_topics
        ]
        if topics_to_create:
            self.create_topics(topics_to_create)
        else:
            self.logger.info("All required topics already exist")

    def describe_topics(self):
        topics_to_describe = [
            self.settings.kafka_input_topic,
            self.settings.kafka_output_topic,
        ]
        topic_metadata = self.admin_client.list_topics(timeout=5).topics
        for topic in topics_to_describe:
            if topic in topic_metadata:
                metadata = topic_metadata[topic]
                self.logger.info(f"Topic: {topic}")
                self.logger.info(f"  Partitions: {len(metadata.partitions)}")
                self.logger.info(f"  Replication Factor: {len(metadata.partitions[0].replicas)}")
            else:
                self.logger.warning(f"Topic {topic} does not exist")
