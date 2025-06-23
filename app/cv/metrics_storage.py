import json
import os
from typing import Any
from functools import lru_cache

from app.utils.logger import setup_logger


class MetricsStorage:
    def __init__(self, metrics_dir_path: str):
        self.metrics_dir = metrics_dir_path
        self.logger = setup_logger("metrics_storage")
        self.exercises = self._scan_available_metrics()

    # TODO: in future how to implement this more properly? (Not by using names of files for exercises list)
    def _scan_available_metrics(self) -> set:
        """Load available metrics from the metrics directory"""
        exercises = set()
        try:
            for filename in os.listdir(self.metrics_dir):
                if filename.endswith(".json"):
                    exercises.add(filename.replace(".json", ''))
        except Exception as e:
            self.logger.error(f"Failed to scan metrics directory: {e}")
        return exercises

    # Legacy methods - not used anymore
    def reload_all_metrics(self) -> None:
        self.exercises = self._scan_available_metrics()
        self.logger.info("Cleared cache for all exercises")

    @lru_cache(maxsize=None)
    def get_metrics_for_exercise_v2(self, config_filename: str) -> dict[str, Any]:
        """Get v2.0 semantic metrics configuration for any exercise"""
        metrics_file = os.path.join(self.metrics_dir, config_filename)

        if not os.path.exists(metrics_file):
            raise FileNotFoundError(f"v2.0 Metrics file not found: {metrics_file}")

        try:
            with open(metrics_file, 'r') as f:
                config = json.load(f)

            # Валидация v2.0 структуры
            if config.get('version') != '2.0':
                raise ValueError(f"Invalid config version. Expected 2.0, got {config.get('version')}")

            if 'error_taxonomy' not in config:
                raise ValueError("v2.0 config missing required 'error_taxonomy' section")

            self.logger.debug(f"Loaded v2.0 config: {config.get('exercise_name', 'unknown')}")
            return config

        except Exception as e:
            self.logger.error(f"Failed to load v2.0 config {config_filename}: {e}")
            raise

    def list_available_exercises_v2(self) -> dict[str, str]:
        """List all available exercise configurations with their versions"""
        exercises = {}

        try:
            for filename in os.listdir(self.metrics_dir):
                if filename.endswith('.json'):
                    try:
                        filepath = os.path.join(self.metrics_dir, filename)
                        with open(filepath, 'r') as f:
                            config = json.load(f)

                        exercise_name = config.get('exercise_name', filename.replace('.json', ''))
                        version = config.get('version', '1.0')
                        exercises[exercise_name] = version

                    except Exception:
                        self.logger.exception("Failed to load v2.0 config %s", filename)
                        continue
        except Exception as e:
            self.logger.error(f"Failed to list exercises: {e}")

        return exercises
