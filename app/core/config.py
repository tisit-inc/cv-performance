import os
import pathlib
import ssl
from functools import lru_cache
from typing import Optional

from pydantic import computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings


class Environment:
    PROD = "prod"
    DEV = "dev"
    TEST = "test"


def get_project_root() -> str:
    return str(pathlib.Path(__file__).parent.parent.parent.absolute())


class BaseAppSettings(BaseSettings):
    """Base settings class with common configuration"""

    model_config = {"case_sensitive": True, "env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


class AppSettings(BaseAppSettings):
    """General application settings"""

    ENV: str = Environment.DEV
    DEBUG: bool = False
    RELEASE_VER: str = "0.1.0"
    PROJECT_NAME: str = "cv-performance"

    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    @property
    def is_development(self) -> bool:
        return self.ENV.lower() == Environment.DEV

    @property
    def is_production(self) -> bool:
        return self.ENV.lower() == Environment.PROD

    @property
    def is_testing(self) -> bool:
        return self.ENV.lower() == Environment.TEST


class BuildSettings(BaseAppSettings):
    """Build-related settings from Docker build args"""

    BUILD_TIME_MOSCOW: str = "unknown"
    GIT_COMMIT: str = "unknown"
    PYTHONUNBUFFERED: str = "1"
    PYTHONPATH: str = "/app"


class PathSettings(BaseAppSettings):
    """Path-related settings"""

    PROJECT_ROOT: str = get_project_root()
    METRICS_DIR: str = f"{get_project_root()}/app/cv/metrics"
    LOGS_DIR: str = f"{get_project_root()}/app/logs"

    # Legacy compatibility
    app_log_directory: str = "app/logs"
    app_log_level: str = "INFO"
    app_log_max_size: int = 10 * 1024 * 1024  # 10 MB
    app_log_backup_count: int = 30
    app_log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class KafkaSettings(BaseAppSettings):
    """Kafka configuration settings for Quix Streams"""

    # Kafka common settings
    KAFKA_CLIENT_ID: str = "performance-worker"
    KAFKA_BROKERS: str = "kafka-1:9092,kafka-2:9092,kafka-3:9092"

    # Consumer group IDs following <env>.<domain>.<component> notation
    KAFKA_GROUP_ID: str | None = None
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_ENABLE_AUTO_COMMIT: bool = True
    KAFKA_MAX_POLL_RECORDS: int = 500
    KAFKA_SESSION_TIMEOUT_MS: int = 30000
    KAFKA_HEARTBEAT_INTERVAL_MS: int = 3000

    # Kafka producer settings
    KAFKA_ACKS: str = "all"  # 0, 1, all
    KAFKA_BATCH_SIZE: int = 16384
    KAFKA_LINGER_MS: int = 5
    KAFKA_COMPRESSION_TYPE: str = "snappy"
    KAFKA_RETRIES: int = 3

    # Topics
    KAFKA_INPUT_TOPIC: str = "cv.inference.results"
    KAFKA_OUTPUT_TOPIC: str = "cv.performance.results"

    # LLM Topics
    KAFKA_LLM_INPUT_TOPIC: str = "llm-coaching-input"
    KAFKA_LLM_OUTPUT_TOPIC: str = "llm-coaching-output"

    # Kafka security
    KAFKA_SECURITY_PROTOCOL: str | None = "SASL_PLAINTEXT"
    KAFKA_SSL_CA: str | None = None
    KAFKA_SSL_CERT: str | None = None
    KAFKA_SSL_KEY: str | None = None
    KAFKA_SASL_MECHANISM: str | None = "SCRAM-SHA-512"
    KAFKA_SASL_USERNAME: str | None = None
    KAFKA_SASL_PASSWORD: str | None = None

    # OAuth 2 (SASL/OAUTHBEARER)
    KAFKA_OAUTH_TOKEN_URL: str | None = None  # https://keycloak/realm/…/token
    KAFKA_OAUTH_CLIENT_ID: str | None = None
    KAFKA_OAUTH_CLIENT_SECRET: str | None = None
    KAFKA_OAUTH_SCOPE: str | None = None

    # Quix Streams application settings
    KAFKA_COMMIT_INTERVAL_SECONDS: float = 5.0
    KAFKA_AUTO_CREATE_TOPICS: bool = True
    KAFKA_TOPIC_CREATE_TIMEOUT_SECONDS: float = 60.0
    KAFKA_AUTO_COMMIT_INTERVAL_MS: int = 1000
    KAFKA_HEALTH_CHECK_TIMEOUT_SECONDS: float = 3.0

    @property
    def kafka_group_id(self) -> str:
        """Group ID following <env>.<domain>.<component> notation"""
        return self.KAFKA_GROUP_ID or f"{self.ENV.lower()}.performance.quality-processor"

    @model_validator(mode="after")
    def auto_fill_protocol(self):
        if self.KAFKA_SECURITY_PROTOCOL is None:
            self.KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT" if self.ENV in {"dev", "test"} else "SASL_SSL"
        return self

    @computed_field
    @property
    def has_ssl_config(self) -> bool:
        return bool(self.KAFKA_SSL_CA and self.KAFKA_SSL_CERT and self.KAFKA_SSL_KEY)

    def get_ssl_context(self) -> ssl.SSLContext | None:
        """Create SSL context for Kafka clients"""
        if not self.has_ssl_config:
            return None
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=self.KAFKA_SSL_CERT, keyfile=self.KAFKA_SSL_KEY)
        context.load_verify_locations(cafile=self.KAFKA_SSL_CA)
        if self.ENV in {"dev", "test"}:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        return context

    def get_oauth_params(self) -> dict[str, str]:
        """Get OAuth parameters for SASL/OAUTHBEARER authentication"""
        if self.KAFKA_SASL_MECHANISM != "OAUTHBEARER":
            return {}
        params = {
            "sasl_oauth_token_endpoint_url": self.KAFKA_OAUTH_TOKEN_URL,
            "sasl_oauth_client_id": self.KAFKA_OAUTH_CLIENT_ID,
            "sasl_oauth_client_secret": self.KAFKA_OAUTH_CLIENT_SECRET,
        }
        if self.KAFKA_OAUTH_SCOPE:
            params["sasl_oauth_scope"] = self.KAFKA_OAUTH_SCOPE
        return params

    @computed_field
    @property
    def brokers_list(self) -> list[str]:
        """Get list of Kafka brokers"""
        return [broker.strip() for broker in self.KAFKA_BROKERS.split(",")]

    @field_validator("KAFKA_BROKERS")
    @classmethod
    def validate_bootstrap_servers(cls, v: str) -> str:
        """Validate Kafka bootstrap servers format"""
        if not v or not v.strip():
            raise ValueError("KAFKA_BROKERS cannot be empty")
        return v.strip()


class LLMSettings(BaseAppSettings):
    """LLM configuration settings"""

    # Gemini API
    GEMINI_API_KEY: str | None = None
    GEMINI_MODEL: str = "gemini-2.5-flash-preview-05-20"

    # Generation settings
    LLM_TEMPERATURE: float = 0.5
    LLM_MAX_OUTPUT_TOKENS: int = 500
    LLM_TOP_P: float = 0.8
    LLM_TOP_K: int = 40


class MonitoringSettings(BaseAppSettings):
    """Monitoring and health check settings"""

    SERVICE_PROMETHEUS_PORT: int | None = None
    HEALTH_CHECK_TIMEOUT: float = 5.0


class Settings(AppSettings, BuildSettings, PathSettings, KafkaSettings, LLMSettings, MonitoringSettings):
    """Main settings class that combines all configuration groups"""

    # Legacy property mappings for compatibility
    @property
    def app_name(self) -> str:
        return self.PROJECT_NAME

    @property
    def debug(self) -> bool:
        return self.DEBUG

    @property
    def kafka_bootstrap_servers(self) -> str:
        return self.KAFKA_BROKERS

    @property
    def kafka_group_id(self) -> str:
        return self.KAFKA_GROUP_ID or f"{self.ENV.lower()}.performance.quality-processor"

    @property
    def kafka_input_topic(self) -> str:
        return self.KAFKA_INPUT_TOPIC

    @property
    def kafka_output_topic(self) -> str:
        return self.KAFKA_OUTPUT_TOPIC

    @property
    def metrics_dir(self) -> str:
        return self.METRICS_DIR

    @property
    def logs_dir(self) -> str:
        return self.LOGS_DIR


@lru_cache()
def get_settings() -> Settings:
    env = os.getenv("ENV", "local")
    env_file = pathlib.Path(get_project_root()) / f".env.{env}"
    if not env_file.exists():
        env_file = pathlib.Path(get_project_root()) / ".env"
    return Settings(_env_file=env_file if env_file.exists() else None)


settings = get_settings()
