# app/cv/metrics_storage.py

import json
import os
from typing import Dict, Any
from pydantic import BaseModel
from functools import lru_cache

from app.utils.logger import setup_logger


class FeedbackMessage(BaseModel):
    id: str
    message: str
    color: str | None = None
    severity: str | None = None
    additional_drawing: list[dict[str, Any]] | None = None


class ExerciseMetrics(BaseModel):
    exercise_name: str
    dominant_side_points: list[str]
    landmark_features_dict: Dict[str, list[str]]
    angles: list[Dict[str, Any]]
    state_machine: Dict[str, Any]
    thresholds: Dict[str, Any]
    inactivity_time: int | None
    feedback_messages: list[FeedbackMessage]


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

    # TODO: точно None по maxsize?
    @lru_cache(maxsize=None)
    def get_metrics_for_exercise(self, exercise_name: str) -> Dict[str, Any]:
        # self.logger.debug(f"Current project directory: {os.getcwd()}")
        if exercise_name not in self.exercises:
            self.logger.warning(f"Metrics for exercise {exercise_name} not found")
            return {}
        try:
            metrics_file = os.path.join(self.metrics_dir, f"{exercise_name}.json")
            with open(metrics_file, 'r') as f:
                metrics = json.load(f)
            ExerciseMetrics(**metrics)

            self.logger.debug(f"Loaded metrics for exercise {exercise_name}")
            return metrics
        except Exception as e:
            self.logger.error(f"Failed to load metrics for exercise {exercise_name}: {e}")
            return {}

    @lru_cache(maxsize=None)
    def get_feedback_messages(self, exercise_name: str) -> Dict[str, FeedbackMessage]:
        """Get feedback messages for the exercise"""
        metrics = self.get_metrics_for_exercise(exercise_name)
        return {
            msg['id']: FeedbackMessage(**msg)
            for msg in metrics.get('feedback_messages', [])
        }

    def get_feedback_message(self, exercise_name: str, feedback_id: str) -> FeedbackMessage | None:
        """Get the exact feedback message by its id for the exercise"""
        messages = self.get_feedback_messages(exercise_name)
        return messages.get(feedback_id)

    # not used by now
    def reload_exercise_metrics(self, exercise_name: str) -> bool:
        if exercise_name not in self.exercises:
            self.get_metrics_for_exercise.cache_clear()
            self.logger.info(f"Cleared cache for exercise: {exercise_name}")
            return True
        return False

    def reload_all_metrics(self) -> None:
        self.exercises = self._scan_available_metrics()
        self.get_metrics_for_exercise.cache_clear()
        self.logger.info("Cleared cache for all exercises")
