import time
from typing import Dict, List, Any, Tuple
import numpy as np


class ExerciseAnalyzer:
    def __init__(self, detection_strategy, angle_calculation_strategy, metrics: Dict[str, Any], level: int = 0):
        # Инициализация из метрик упражнения
        self.exercise_name = metrics['exercise_name']
        self.landmark_features_dict = metrics['landmark_features_dict']
        self.angles = metrics['angles']
        self.state_machine = metrics['state_machine']
        self.thresholds_beginner = metrics['thresholds']['beginner']
        self.thresholds_pro = metrics['thresholds']['pro']
        self.inactivity_time = metrics['inactivity_time']
        self.dominant_side_points = metrics['dominant_side_points']

        self.current_state = self.state_machine["beginner" if level == 0 else "pro"]["states"][0]['id']
        self.set_level(level)

        self.detector = detection_strategy
        self.angle_calculation = angle_calculation_strategy

    def set_level(self, level: int = 0) -> None:
        """Установка уровня сложности упражнения"""
        self.thresholds = self.thresholds_pro if level else self.thresholds_beginner

    def get_keypoint_coords(self, keypoints) -> Dict[str, Tuple[int, int]]:
        """Получение координат ключевых точек"""
        keypoint_coords = {}
        for feature_name, feature_keypoints in self.landmark_features_dict.items():
            if feature_name == "nose":
                keypoint_coords["nose"] = self.detector.get_landmark_coordinates("nose")
            else:
                coords = self.detector.get_landmark_coordinates(feature_name)
                for i, keypoint in enumerate(feature_keypoints):
                    keypoint_coords[f"{feature_name}_{keypoint}"] = coords[i]
        return keypoint_coords

    def calculate_angle(self, angle_data: Dict[str, Any], keypoint_coords: Dict[str, Tuple[int, int]]) -> float:
        """Расчет угла между точками"""
        if "vertical" in angle_data and angle_data["vertical"]:
            # Vertical angle calculation
            dominant_side = self.get_dominant_side(keypoint_coords)
            if not angle_data['point1'].startswith(('left_', 'right_')):
                p1 = keypoint_coords[f"{dominant_side}_{angle_data['point1']}"]
                p2 = keypoint_coords[f"{dominant_side}_{angle_data['point2']}"]
            else:
                p1 = keypoint_coords[angle_data['point1']]
                p2 = keypoint_coords[angle_data['point2']]
            return abs((angle_data.get("direction") == "down") * (-180) + \
                       self.angle_calculation.calculate_angle(p2, np.array([p2[0], 0]), p1))
        else:
            # Standard angle calculation
            dominant_side = self.get_dominant_side(keypoint_coords)
            if not angle_data['point1'].startswith(('left_', 'right_')):
                p1 = keypoint_coords[f"{dominant_side}_{angle_data['point1']}"]
            else:
                p1 = keypoint_coords[angle_data['point1']]
            if not angle_data['point2'].startswith(('left_', 'right_')):
                p2 = keypoint_coords[f"{dominant_side}_{angle_data['point2']}"]
            else:
                p2 = keypoint_coords[angle_data['point2']]
            if angle_data['ref_point'] == "nose":
                ref_pt = keypoint_coords["nose"]
            else:
                if not angle_data['ref_point'].startswith(('left_', 'right_')):
                    ref_pt = keypoint_coords[f"{dominant_side}_{angle_data['ref_point']}"]
                else:
                    ref_pt = keypoint_coords[angle_data['ref_point']]
            return self.angle_calculation.calculate_angle(p1, p2, ref_pt)

    def get_dominant_side(self, keypoint_coords: Dict[str, Tuple[int, int]]) -> str:
        """Определение доминантной стороны"""
        return 'right'  # Временно для тестирования

    def analyze_frame(self, frame_data: Dict[str, Any], current_state: Dict[str, Any]) -> Dict[str, Any]:
        """Анализ кадра и возвращение обновленного состояния"""
        keypoints = self.detector.get_coordinates()

        if not len(keypoints):
            return self._handle_no_keypoints(current_state)

        # Получаем координаты и углы
        keypoint_coords = self.get_keypoint_coords(keypoints)
        angles = {}
        for angle_data in self.angles:
            angles[angle_data["name"]] = self.calculate_angle(angle_data, keypoint_coords)

        # Проверяем позу
        self._check_offset_angle(angles, current_state)

        # Проверяем неактивность
        display_inactivity = self._check_inactivity(current_state)

        if current_state['flags']['incorrect_posture']:
            return self._handle_incorrect_posture(current_state, angles)

        # Определяем новое состояние и обновляем последовательность
        new_state = self.get_current_state(angles)
        current_state['curr_state'] = new_state
        self._update_state_sequence(current_state, new_state)

        # Проверяем пороги и генерируем фидбек
        feedback_ids = self._check_thresholds(angles, current_state)

        # Проверяем корректность последовательности состояний
        incorrect_sequence = self._check_sequence_correctness(current_state)

        # Обрабатываем завершение повторения
        if new_state == self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"][
            "success_sequence"][0]:
            if not current_state['flags']['incorrect_posture'] and not current_state['flags'][
                'critical_error'] and not incorrect_sequence:
                current_state['curls']['correct'] += 1
            else:
                current_state['curls']['incorrect'] += 1
            current_state['state_seq'] = []
            current_state['flags']['incorrect_posture'] = False
            current_state['flags']['critical_error'] = False

        # Обновляем предыдущее состояние
        current_state['prev_state'] = new_state

        return {
            'state': current_state,
            'feedback_ids': feedback_ids,
            'angles': angles,
            'display_inactivity': display_inactivity
        }

    def _handle_no_keypoints(self, current_state: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка ситуации отсутствия ключевых точек"""
        end_time = time.perf_counter()
        current_state['timers']['inactive_time'] += end_time - current_state['timers']['start_inactive_time']

        if current_state['timers']['inactive_time'] >= self.thresholds['INACTIVE_THRESH']:
            current_state['curls']['correct'] = 0
            current_state['curls']['incorrect'] = 0

        current_state['timers']['start_inactive_time'] = end_time
        current_state['prev_state'] = None
        current_state['curr_state'] = None
        current_state['timers']['inactive_time_front'] = 0.0
        current_state['flags']['incorrect_posture'] = False
        current_state['state_seq'] = []

        return {
            'state': current_state,
            'feedback_ids': [],
            'angles': {},
            'display_inactivity': True
        }

    def _handle_incorrect_posture(self, current_state: Dict[str, Any], angles: Dict[str, float]) -> Dict[str, Any]:
        """Обработка неправильной позы"""
        end_time = time.perf_counter()
        current_state['timers']['inactive_time_front'] += end_time - current_state['timers'][
            'start_inactive_time_front']
        current_state['timers']['start_inactive_time_front'] = end_time

        display_inactivity = False
        if current_state['timers']['inactive_time_front'] >= self.thresholds['INACTIVE_THRESH']:
            current_state['curls']['correct'] = 0
            current_state['curls']['incorrect'] = 0
            display_inactivity = True

        current_state['timers']['start_inactive_time'] = time.perf_counter()
        current_state['timers']['inactive_time'] = 0.0
        current_state['prev_state'] = None
        current_state['curr_state'] = None

        return {
            'state': current_state,
            'feedback_ids': [],
            'angles': angles,
            'display_inactivity': display_inactivity
        }

    def _check_inactivity(self, current_state: Dict[str, Any]) -> bool:
        """Проверка неактивности"""
        if current_state['curr_state'] == current_state['prev_state']:
            end_time = time.perf_counter()
            current_state['timers']['inactive_time'] += end_time - current_state['timers']['start_inactive_time']
            current_state['timers']['start_inactive_time'] = end_time

            if current_state['timers']['inactive_time'] >= self.thresholds['INACTIVE_THRESH']:
                current_state['curls']['correct'] = 0
                current_state['curls']['incorrect'] = 0
                current_state['timers']['inactive_time'] = 0.0
                current_state['timers']['start_inactive_time'] = time.perf_counter()
                return True
        else:
            current_state['timers']['start_inactive_time'] = time.perf_counter()
            current_state['timers']['inactive_time'] = 0.0
        return False

    def _check_offset_angle(self, angles: Dict[str, float], current_state: Dict[str, Any]) -> None:
        """Проверка угла отклонения"""
        threshold = self.thresholds['OFFSET_THRESH']
        angle_name = next((angle["name"] for angle in self.angles if "offset" in angle["name"]), None)
        if angle_name and abs(angles[angle_name]) > threshold:
            current_state['flags']['incorrect_posture'] = True
        else:
            current_state['flags']['incorrect_posture'] = False

    def _check_thresholds(self, angles: Dict[str, float], current_state: Dict[str, Any]) -> List[str]:
        """Проверка пороговых значений углов"""
        feedback_ids = []
        has_critical_error = False

        if current_state['curr_state'] != 's1':
            for angle_name, thresholds in self.thresholds["angle_thresh"].items():
                angle_value = angles[angle_name]
                for threshold in thresholds:
                    if ((threshold["min"] is not None and angle_value < threshold["min"]) or
                            (threshold["max"] is not None and angle_value > threshold["max"])):
                        feedback_ids.append(threshold['feedback'])
                        if threshold.get("critical"):
                            has_critical_error = True

        current_state['flags']['critical_error'] = has_critical_error
        return feedback_ids

    def _update_state_sequence(self, current_state: Dict[str, Any], new_state: str) -> None:
        """Обновление последовательности состояний"""
        success_sequence = self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"][
            "success_sequence"]
        if new_state in success_sequence:
            if len(current_state['state_seq']) == 0 or new_state != current_state['state_seq'][-1]:
                current_state['state_seq'].append(new_state)
                if len(current_state['state_seq']) > len(success_sequence):
                    current_state['state_seq'] = current_state['state_seq'][1:]

    def _check_sequence_correctness(self, current_state: Dict[str, Any]) -> bool:
        """Проверка корректности последовательности состояний"""
        expected_state_index = len(current_state['state_seq']) - 1
        success_sequence = self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"][
            "success_sequence"]

        if expected_state_index >= 0 and expected_state_index < len(success_sequence):
            expected_state = success_sequence[expected_state_index]
            return current_state['curr_state'] != expected_state
        return False

    def get_current_state(self, angles: Dict[str, float]) -> str:
        """Определение текущего состояния на основе углов"""
        states = self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"]["states"]
        for state in states:
            if eval(state["condition"], {"__builtins__": None}, angles):
                return state["id"]
        return None
