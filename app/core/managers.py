"""
Stream processing managers for cv-performance
Centralized Quix Streams configuration and management
"""

import logging
from quixstreams import Application
from quixstreams.models import Topic

from app.core.config import get_settings
from app.utils.logger import setup_logger


class QuixStreamsManager:
    """
    Centralized Quix Streams manager for stream processing
    Handles Application configuration and lifecycle management
    """

    def __init__(self, logger: logging.Logger | None = None):
        self.settings = get_settings()
        self.logger = logger or setup_logger(__name__)
        self._app: Application | None = None
        self._topics: dict[str, Topic] = {}

    def _create_application(self) -> Application:
        """Create and configure Quix Streams Application"""

        app_config = {
            "broker_address": self.settings.kafka_bootstrap_servers,
            "consumer_group": self.settings.kafka_group_id,
            "auto_offset_reset": self.settings.KAFKA_AUTO_OFFSET_RESET,
            "commit_interval": self.settings.KAFKA_COMMIT_INTERVAL_SECONDS,
            "auto_create_topics": self.settings.KAFKA_AUTO_CREATE_TOPICS,
            "topic_create_timeout": self.settings.KAFKA_TOPIC_CREATE_TIMEOUT_SECONDS,
        }

        producer_config = {
            "compression.type": self.settings.KAFKA_COMPRESSION_TYPE,
            "batch.size": self.settings.KAFKA_BATCH_SIZE,
            "linger.ms": self.settings.KAFKA_LINGER_MS,
            "acks": self.settings.KAFKA_ACKS,
            "retries": self.settings.KAFKA_RETRIES,
        }

        consumer_config = {
            "session.timeout.ms": self.settings.KAFKA_SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": self.settings.KAFKA_HEARTBEAT_INTERVAL_MS,
            "enable.auto.commit": self.settings.KAFKA_ENABLE_AUTO_COMMIT,
            "auto.commit.interval.ms": self.settings.KAFKA_AUTO_COMMIT_INTERVAL_MS,
        }

        # Configure security settings if available
        security_protocol = getattr(self.settings, 'KAFKA_SECURITY_PROTOCOL', None)
        if security_protocol:
            consumer_config["security.protocol"] = security_protocol
            producer_config["security.protocol"] = security_protocol

            sasl_mechanism = getattr(self.settings, 'KAFKA_SASL_MECHANISM', None)
            if sasl_mechanism:
                consumer_config["sasl.mechanism"] = sasl_mechanism
                producer_config["sasl.mechanism"] = sasl_mechanism

                sasl_username = getattr(self.settings, 'KAFKA_SASL_USERNAME', None)
                if sasl_username:
                    consumer_config["sasl.username"] = sasl_username
                    producer_config["sasl.username"] = sasl_username

                sasl_password = getattr(self.settings, 'KAFKA_SASL_PASSWORD', None)
                if sasl_password:
                    consumer_config["sasl.password"] = sasl_password
                    producer_config["sasl.password"] = sasl_password

        if self.settings.KAFKA_SSL_CA:
            consumer_config["ssl.ca.location"] = self.settings.KAFKA_SSL_CA
            producer_config["ssl.ca.location"] = self.settings.KAFKA_SSL_CA

        if self.settings.KAFKA_SSL_CERT:
            consumer_config["ssl.certificate.location"] = self.settings.KAFKA_SSL_CERT
            producer_config["ssl.certificate.location"] = self.settings.KAFKA_SSL_CERT

        if self.settings.KAFKA_SSL_KEY:
            consumer_config["ssl.key.location"] = self.settings.KAFKA_SSL_KEY
            producer_config["ssl.key.location"] = self.settings.KAFKA_SSL_KEY

        oauth_params = self.settings.get_oauth_params()
        if oauth_params:
            consumer_config.update(oauth_params)
            producer_config.update(oauth_params)

        def on_consumer_error(exc: Exception, message, logger) -> bool:
            logger.error("Consumer error: %s, offset: %s", exc, getattr(message, 'offset', 'unknown'))
            # Return False to propagate critical errors, True to ignore recoverable ones
            return False

        def on_processing_error(exc: Exception, row, logger) -> bool:
            logger.error("Processing error: %s, row: %s", exc, row)
            # Log and continue processing for data errors
            return True

        def on_producer_error(exc: Exception, row, logger) -> bool:
            logger.error("Producer error: %s, row: %s", exc, row)
            # Return False for producer errors to ensure data consistency
            return False

        # Add client_id to both configs
        if self.settings.KAFKA_CLIENT_ID:
            consumer_config["client.id"] = f"{self.settings.KAFKA_CLIENT_ID}-consumer"
            producer_config["client.id"] = f"{self.settings.KAFKA_CLIENT_ID}-producer"

        app_config.update({
            "producer_extra_config": producer_config,
            "consumer_extra_config": consumer_config,
            "on_consumer_error": on_consumer_error,
            "on_processing_error": on_processing_error,
            "on_producer_error": on_producer_error,
        })

        self.logger.info(
            "Creating Quix Streams Application with config: %s",
            {k: v for k, v in app_config.items() if 'password' not in k.lower()},
        )

        return Application(**app_config)

    @property
    def app(self) -> Application:
        """Get or create Quix Streams Application instance"""
        if self._app is None:
            self._app = self._create_application()
            self.logger.info("Quix Streams Application initialized")
        return self._app

    def get_topic(self, topic_name: str, **topic_config) -> Topic:
        """Get or create a topic with configuration"""
        if topic_name not in self._topics:
            self.logger.info("Creating topic: %s", topic_name)
            self._topics[topic_name] = self.app.topic(topic_name, **topic_config)
        return self._topics[topic_name]

    def get_input_topic(self, **config) -> Topic:
        """Get input topic for performance data"""
        return self.get_topic(self.settings.kafka_input_topic, **config)

    def get_output_topic(self, **config) -> Topic:
        """Get output topic for performance feedback"""
        return self.get_topic(self.settings.kafka_output_topic, **config)

    def create_dataframe(self, topic: Topic):
        """Create streaming dataframe from topic"""
        return self.app.dataframe(topic)

    def run(self):
        """Run the Quix Streams application"""
        self.logger.info("Starting Quix Streams Application")
        try:
            self.app.run()
        except KeyboardInterrupt:
            self.logger.info("Quix Streams Application stopped by user")
        except Exception as e:
            self.logger.exception("Quix Streams Application error: %s", e)
            raise
        finally:
            self.logger.info("Quix Streams Application shutdown complete")

    def stop(self):
        """Stop the Quix Streams application gracefully"""
        if self._app:
            self.logger.info("Stopping Quix Streams Application")
            self.app.stop()

    def get_consumer(self):
        return self.app.get_consumer()

    def get_producer(self):
        return self.app.get_producer()

    @property
    def is_running(self) -> bool:
        return self._app is not None

    def health_check(self) -> dict[str, object]:
        try:
            with self.get_producer() as producer:
                metadata = producer.list_topics(timeout=self.settings.KAFKA_HEALTH_CHECK_TIMEOUT_SECONDS)

            return {
                "status": "healthy",
                "broker_address": self.settings.kafka_bootstrap_servers,
                "consumer_group": self.settings.kafka_group_id,
                "topics_available": len(metadata.topics) if metadata else 0,
                "application_initialized": self._app is not None,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "broker_address": self.settings.kafka_bootstrap_servers,
                "application_initialized": self._app is not None,
            }
