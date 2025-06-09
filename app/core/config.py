import os
import pathlib
from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
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
    RELEASE_VER: str = "3.0.0"
    PROJECT_NAME: str = "tisit-performance-svc"

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
    """Build-related settings"""

    BUILD_TIME_MOSCOW: str = "unknown"
    GIT_COMMIT: str = "unknown"


class PathSettings(BaseAppSettings):
    """Path-related settings"""

    PROJECT_ROOT: str = get_project_root()
    METRICS_DIR: str = "app/cv/metrics"
    LOGS_DIR: str = "app/logs"

    # Legacy compatibility
    app_log_directory: str = "app/logs"
    app_log_level: str = "INFO"
    app_log_max_size: int = 10 * 1024 * 1024  # 10 MB
    app_log_backup_count: int = 30
    app_log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class KafkaSettings(BaseAppSettings):
    """Kafka configuration settings"""

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_GROUP_ID: str = "performance-group-universal"
    KAFKA_INPUT_TOPIC: str = "performance-input"
    KAFKA_OUTPUT_TOPIC: str = "performance-feedback"

    # Connection settings
    KAFKA_MAX_POLL_RECORDS: int = 500
    KAFKA_SESSION_TIMEOUT_MS: int = 10000
    KAFKA_HEARTBEAT_INTERVAL_MS: int = 3000

    @field_validator("KAFKA_BOOTSTRAP_SERVERS")
    @classmethod
    def validate_bootstrap_servers(cls, v: str) -> str:
        """Validate Kafka bootstrap servers format"""
        if not v or not v.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS cannot be empty")
        return v.strip()


class MonitoringSettings(BaseAppSettings):
    """Monitoring and health check settings"""

    SERVICE_PROMETHEUS_PORT: Optional[int] = None
    HEALTH_CHECK_TIMEOUT: float = 5.0


class Settings(
    AppSettings, BuildSettings, PathSettings, KafkaSettings, MonitoringSettings
):
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
        return self.KAFKA_BOOTSTRAP_SERVERS

    @property
    def kafka_group_id(self) -> str:
        return self.KAFKA_GROUP_ID

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