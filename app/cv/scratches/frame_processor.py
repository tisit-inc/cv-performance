# app/cv/frame_processor.py

from typing import Dict, Any, List, Optional
from app.utils.logger import setup_logger


class FrameProcessor:
    def __init__(self, metrics_storage):
        self.metrics_storage = metrics_storage
        self.logger = setup_logger("frame_processor")

    # TODO: подумать над необходимостью реализации метода
    def validate_frame_data(self, frame_data: Dict[str, Any], exercise_metrics: Dict[str, Any]) -> bool:
        """Проверяет наличие всех необходимых данных для анализа."""
        # try:
        #     required_angles = exercise_metrics.get('required_angles', [])
        #     frame_angles = [angle['name'] for angle in frame_data['metrics']['angles']]
        #
        #     missing_angles = set(required_angles) - set(frame_angles)
        #     if missing_angles:
        #         self.logger.warning(f"Missing required angles: {missing_angles}")
        #         return False
        #
        #     return True
        # except KeyError as e:
        #     self.logger.error(f"Invalid frame data structure: {e}")
        #     return False
        return True

    def extract_required_data(self, frame_data: Dict[str, Any], exercise_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Извлекает необходимые данные для анализа упражнения."""
        required_angles = exercise_metrics.get('required_angles', [])
        required_landmarks = exercise_metrics.get('required_landmarks', [])

        # Фильтруем только нужные углы
        angles_data = {
            angle['name']: angle['value']
            for angle in frame_data['metrics']['angles']
            if angle['name'] in required_angles
        }

        # Фильтруем только нужные landmarks
        landmarks_data = {
            i: landmark
            for i, landmark in enumerate(frame_data['landmarks'])
            if i in required_landmarks
        }

        return {
            'angles': angles_data,
            'landmarks': landmarks_data
        }

    async def process_frame(self, frame_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Обрабатывает один кадр."""
        try:
            exercise_name = frame_data['exercise']
            exercise_metrics = self.metrics_storage.get_metrics_for_exercise(exercise_name)

            if not exercise_metrics:
                self.logger.error(f"No metrics found for exercise: {exercise_name}")
                return None

            if not self.validate_frame_data(frame_data, exercise_metrics):
                return None

            extracted_data = self.extract_required_data(frame_data, exercise_metrics)

            processed_frame = {
                'frame_track_uuid': frame_data['frame_track_uuid'],
                'timestamp': frame_data['timestamp'],
                'exercise': exercise_name,
                'data': extracted_data,
                'thresholds': exercise_metrics.get('thresholds', {}),
                'phase_definitions': exercise_metrics.get('phase_definitions', {})
            }

            self.logger.debug(f"Successfully processed frame {frame_data['frame_track_uuid']}")
            return processed_frame

        except Exception as e:
            self.logger.error(f"Error processing frame: {e}", exc_info=True)
            return None

    async def process_frames(self, frames: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Обрабатывает список кадров."""
        processed_frames = []
        for frame in frames:
            processed_frame = await self.process_frame(frame)
            if processed_frame:
                processed_frames.append(processed_frame)

        return processed_frames
