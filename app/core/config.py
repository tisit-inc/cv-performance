import os
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    # Kafka settings
    kafka_bootstrap_servers: str
    kafka_input_topic: str = "inference_results"
    kafka_output_topic: str = "performance-feedback" 
    kafka_group_id: str

    # redis / dragonfly settings
    redis_url: str

    # App settings
    app_log_level: str = "INFO"
    app_log_directory: str = "logs"
    app_log_max_size: int = 10 * 1024 * 1024  # 10 MB
    app_log_backup_count: int = 30
    app_log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Buffer settings
    buffer_size: int = 5
    buffer_window: int = 1

    metrics_dir: str = 'cv/metrics'


@lru_cache()
def get_settings():
    env = os.getenv("ENV", "local")
    env_file = BASE_DIR / f".env.{env}"
    if not env_file.exists():
        env_file = BASE_DIR / ".env"
    return Settings(_env_file=env_file)
