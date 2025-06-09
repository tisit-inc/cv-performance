import json
import time
import numpy as np
import cv2

from cv_module.src.models import opencv_elements


class ExerciseProcessor:
    def __init__(self, detection_strategy, angle_calculation_strategy, exercise_json_path, level=0):
        with open(exercise_json_path) as f:
            exercise_data = json.load(f)

        self.exercise_name = exercise_data['exercise_name']
        self.landmark_features_dict = exercise_data['landmark_features_dict']
        self.angles = exercise_data['angles']
        self.state_machine = exercise_data['state_machine']
        self.thresholds_beginner = exercise_data['thresholds']['beginner']
        self.thresholds_pro = exercise_data['thresholds']['pro']
        self.feedback_messages = exercise_data['feedback_messages']
        self.inactivity_time = exercise_data['inactivity_time']
        self.dominant_side_points = exercise_data['dominant_side_points']

        self.current_state = self.state_machine["beginner" if level == 0 else "pro"]["states"][0]['id']
        self.set_level(level)

        # self.detector = detection_strategy
        # self.angle_calculation = angle_calculation_strategy
        # self.cv_elem = opencv_elements.OpenCVElements

        # Font type
        # self.font = cv2.FONT_HERSHEY_SIMPLEX
        # line type
        # self.linetype = cv2.LINE_AA

        # self.COLORS = {
        #     'black': (14, 16, 15),
        #     'blue': (0, 127, 255),
        #     'red': (255, 50, 50),
        #     'green': (0, 255, 127),
        #     'light_green': (100, 233, 127),
        #     'yellow': (255, 255, 0),
        #     'magenta': (255, 0, 255),
        #     'white': (255, 255, 255),
        #     'cyan': (0, 255, 255),
        #     'light_blue': (102, 204, 255),
        #     'purple': (143, 126, 213),
        #     'pink': (229, 156, 209)
        # }

        self.state_tracker = {
            'state_seq': [],
            'start_inactive_time': time.perf_counter(),
            'start_inactive_time_front': time.perf_counter(),
            'INACTIVE_TIME': 0.0,
            'INACTIVE_TIME_FRONT': 0.0,
            'INACTIVE_TIME_START': 0.0,
            'DISPLAY_TEXT': np.full((len(self.feedback_messages),), False),
            'COUNT_FRAMES': np.zeros((len(self.feedback_messages),), dtype=np.int64),
            'INCORRECT_POSTURE': False,
            'CRITICAL_ERROR': False,
            'prev_state': None,
            'curr_state': None,
            'CURLS': 0,
            'BAD_CURLS': 0
        }

    def set_level(self, level=0):
        if level:
            self.thresholds = self.thresholds_pro
        else:
            self.thresholds = self.thresholds_beginner

    def get_keypoint_coords(self, keypoints):
        keypoint_coords = {}
        for feature_name, feature_keypoints in self.landmark_features_dict.items():
            if feature_name == "nose":
                keypoint_coords["nose"] = self.detector.get_landmark_coordinates("nose")
            else:
                coords = self.detector.get_landmark_coordinates(feature_name)
                for i, keypoint in enumerate(feature_keypoints):
                    keypoint_coords[f"{feature_name}_{keypoint}"] = coords[i]
        print("\nKeypoint coordinates:", keypoint_coords)
        return keypoint_coords

    # def calculate_angle(self, angle_data, keypoint_coords):
    #     if "vertical" in angle_data and angle_data["vertical"]:
    #         # Vertical angle calculation
    #         dominant_side = self.get_dominant_side(keypoint_coords)
    #         #  Исправление:  проверяем  наличие  префикса  в  angle_data['point1']
    #         if not angle_data['point1'].startswith(('left_', 'right_')):
    #             p1 = keypoint_coords[f"{dominant_side}_{angle_data['point1']}"]
    #             p2 = keypoint_coords[f"{dominant_side}_{angle_data['point2']}"]
    #         else:  # Если  префикс  уже  есть,  используем  angle_data['point1']  как  есть
    #             p1 = keypoint_coords[angle_data['point1']]
    #             p2 = keypoint_coords[angle_data['point2']]
    #         return abs((angle_data.get("direction") == "down") * (-180) + \
    #                    self.angle_calculation.calculate_angle(p2, np.array([p2[0], 0]), p1))
    #     else:
    #         # Standard angle calculation
    #         dominant_side = self.get_dominant_side(keypoint_coords)
    #         #  Исправление:  проверяем  наличие  префикса  в  angle_data['point1'],  angle_data['point2']  и  angle_data['ref_point']
    #         if not angle_data['point1'].startswith(('left_', 'right_')):
    #             p1 = keypoint_coords[f"{dominant_side}_{angle_data['point1']}"]
    #         else:
    #             p1 = keypoint_coords[angle_data['point1']]
    #         if not angle_data['point2'].startswith(('left_', 'right_')):
    #             p2 = keypoint_coords[f"{dominant_side}_{angle_data['point2']}"]
    #         else:
    #             p2 = keypoint_coords[angle_data['point2']]
    #         if angle_data['ref_point'] == "nose":
    #             ref_pt = keypoint_coords["nose"]  # Используем "nose" без префикса
    #         else:
    #             if not angle_data['ref_point'].startswith(('left_', 'right_')):
    #                 ref_pt = keypoint_coords[f"{dominant_side}_{angle_data['ref_point']}"]
    #             else:
    #                 ref_pt = keypoint_coords[angle_data['ref_point']]
    #         return self.angle_calculation.calculate_angle(p1, p2, ref_pt)

    def get_dominant_side(self, keypoint_coords):
        #  Получаем  названия  точек  из  dominant_side_points
        point1, point2 = self.dominant_side_points
        #  Определяем  dominant_side  на  основе  положения  указанных  точек
        # return "left" if keypoint_coords[f"left_{point1}"][1] > keypoint_coords[f"right_{point2}"][1] else "right"
        return 'right'  # Исправление: возвращаем 'right' для тестирования

    def check_offset_angle(self, angles):
        threshold = self.thresholds['OFFSET_THRESH']
        angle_name = next((angle["name"] for angle in self.angles if "offset" in angle["name"]), None)
        if angle_name and abs(angles[angle_name]) > threshold:
            self.state_tracker['INCORRECT_POSTURE'] = True
        else:
            print(type(self.state_tracker))
            self.state_tracker['INCORRECT_POSTURE'] = False

    def check_inactivity(self):
        if self.state_tracker['curr_state'] == self.state_tracker['prev_state']:
            end_time = time.perf_counter()
            self.state_tracker['INACTIVE_TIME'] += end_time - self.state_tracker['start_inactive_time']
            self.state_tracker['start_inactive_time'] = end_time

            if self.state_tracker['INACTIVE_TIME'] >= self.thresholds['INACTIVE_THRESH']:
                self.state_tracker['CURLS'] = 0
                self.state_tracker['BAD_CURLS'] = 0
                self.state_tracker['INACTIVE_TIME'] = 0.0
                self.state_tracker['start_inactive_time'] = time.perf_counter()
                return True
        else:
            self.state_tracker['start_inactive_time'] = time.perf_counter()
            self.state_tracker['INACTIVE_TIME'] = 0.0
        return False

    def get_current_state(self, angles):
        #  Получаем состояния для текущего уровня сложности
        states = self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"]["states"]
        for state in states:
            if eval(state["condition"], {"__builtins__": None}, angles):
                return state["id"]
        return None

    def check_thresholds_and_draw_feedback(self, frame, angles, keypoint_coords):
        has_critical_error = False
        if self.current_state != 's1':
            for angle_name, thresholds in self.thresholds["angle_thresh"].items():
                angle_value = angles[angle_name]
                for j, threshold in enumerate(thresholds):
                    if threshold["min"] is not None and angle_value < threshold["min"]:
                        # self.state_tracker['DISPLAY_TEXT'][threshold['feedback']] = True  # Активируем  фидбек
                        if threshold["critical"]:
                            has_critical_error = True
                    if threshold["max"] is not None and angle_value > threshold["max"]:
                        # self.state_tracker['DISPLAY_TEXT'][threshold['feedback']] = True  # Активируем  фидбек
                        if threshold["critical"]:
                            has_critical_error = True

        self._show_feedback(frame, keypoint_coords)  # Передаем  только  keypoint_coords
        return has_critical_error

    # def _show_feedback(self, frame, keypoint_coords):
    #     self.state_tracker['COUNT_FRAMES'][
    #         self.state_tracker['DISPLAY_TEXT']] += 1  # Увеличиваем счетчик для активных фидбеков
    #
    #     # Начальная позиция по y для первого сообщения
    #     y_position = 50
    #     # Расстояние между сообщениями
    #     message_spacing = 60
    #
    #     # Проверяем, что текущее состояние не s1
    #     if self.current_state != 's1':
    #         for i, display_feedback in enumerate(self.state_tracker['DISPLAY_TEXT']):
    #             if display_feedback:  # Если фидбек активен
    #                 message = self.feedback_messages[i]  # Получаем сообщение по индексу
    #                 # Вычисляем позицию уведомления с учетом сдвига
    #                 pos = (int(frame.shape[1] * 0.06), y_position)
    #                 self.cv_elem.draw_text(
    #                     frame,
    #                     message["message"],
    #                     pos=pos,  # Используем вычисленную позицию
    #                     text_color=(255, 255, 230),
    #                     font_scale=1,
    #                     font_thickness=3,
    #                     text_color_bg=message["color"][1:],
    #                     increased_size=3
    #                 )
    #                 if "additional_drawing" in message:
    #                     for drawing in message["additional_drawing"]:
    #                         if drawing["type"] == "line":
    #                             dominant_side = self.get_dominant_side(keypoint_coords)
    #                             p1 = keypoint_coords[f"{dominant_side}_{drawing['point1']}"]
    #                             p2 = keypoint_coords[f"{dominant_side}_{drawing['point2']}"]
    #                             color = drawing["color"]
    #                             thickness = drawing["thickness"]
    #                             cv2.line(frame, p1, p2, color, thickness, lineType=self.linetype)
    #
    #                 # Увеличиваем сдвиг по y для следующего уведомления
    #                 y_position += message_spacing
    #
    #     # Деактивируем фидбек, если счетчик превысил CNT_FRAME_THRESH
    #     self.state_tracker['DISPLAY_TEXT'][
    #         self.state_tracker['COUNT_FRAMES'] > self.thresholds['CNT_FRAME_THRESH']] = False
    #     self.state_tracker['COUNT_FRAMES'][
    #         self.state_tracker['COUNT_FRAMES'] > self.thresholds['CNT_FRAME_THRESH']] = 0
    #
    #     return frame

    # def draw_landmark_line(self, frame, p1, p2, color, thickness):
    #     if not np.array_equal(p1, [0, 0]) and not np.array_equal(p2, [0, 0]):
    #         cv2.line(frame, p1, p2, color, thickness, lineType=self.linetype)
    #     return frame

    # def draw_curls_counter(self, frame, curls):
    #     try:
    #         frame_height, frame_width, _ = frame.shape
    #         self.cv_elem.draw_text(
    #             frame,
    #             "CORRECT " + str(self.state_tracker['CURLS']),
    #             pos=(int(frame_width * 0.06), int(frame_height - 80)),
    #             text_color=(10, 228, 72),
    #             font_scale=1,
    #             font_thickness=3,
    #             text_color_bg=self.COLORS['black'],
    #             increased_size=3
    #         )
    #         self.cv_elem.draw_text(
    #             frame,
    #             str(self.state_tracker['BAD_CURLS']) + " INCORRECT",
    #             pos=(int(frame_width * 0.78), int(frame_height - 80)),
    #             text_color=(254, 197, 251),
    #             font_scale=1,
    #             font_thickness=3,
    #             text_color_bg=self.COLORS['black'],
    #             increased_size=3
    #         )
    #         self.cv_elem.draw_text(
    #             frame,
    #             "CURLS: " + str(self.state_tracker['CURLS'] + self.state_tracker['BAD_CURLS']) + \
    #             (curls is not None) * ('/' + str(curls)),
    #             pos=(int(frame_width / 2.8), frame_height - 45),
    #             text_color=(255, 255, 255),
    #             font_scale=0.7,
    #             font_thickness=2,
    #             text_color_bg=self.COLORS['black'])
    #         # pos = (int(frame_width / 2.1), frame_height - 30),
    #     except Exception as e:
    #         pass

    def _update_state_sequence(self, state):
        success_sequence = self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"][
            "success_sequence"]
        if state in success_sequence:
            # Добавляем состояние в state_seq только если оно отличается от предыдущего
            if len(self.state_tracker['state_seq']) == 0 or state != self.state_tracker['state_seq'][-1]:
                self.state_tracker['state_seq'].append(state)

                # Если длина state_seq превышает длину success_sequence, удаляем первый элемент
                if len(self.state_tracker['state_seq']) > len(success_sequence):
                    self.state_tracker['state_seq'] = self.state_tracker['state_seq'][1:]

    def process(self, frame: np.array, curls=None, show_fps=False, plot=False):
        pTime = 0
        play_sound = None

        frame_height, frame_width, _ = frame.shape

        keypoints = self.detector.get_coordinates()

        if len(keypoints):
            # Get keypoint coordinates
            keypoint_coords = self.get_keypoint_coords(keypoints)
            # print(keypoint_coords)
            # Calculate angles
            angles = {}
            for angle_data in self.angles:
                angles[angle_data["name"]] = self.calculate_angle(angle_data, keypoint_coords)

            self.check_offset_angle(angles)

            display_inactivity = False

            if self.state_tracker['INCORRECT_POSTURE']:
                # ... Handle incorrect posture (similar to existing code)
                end_time = time.perf_counter()
                self.state_tracker['INACTIVE_TIME_FRONT'] += end_time - self.state_tracker['start_inactive_time_front']
                self.state_tracker['start_inactive_time_front'] = end_time
                #  Используем  уже  объявленную  переменную  display_inactivity
                if self.state_tracker['INACTIVE_TIME_FRONT'] >= self.thresholds['INACTIVE_THRESH']:
                    self.state_tracker['CURLS'] = 0
                    self.state_tracker['BAD_CURLS'] = 0
                    display_inactivity = True
                cv2.circle(frame, keypoint_coords["nose"], 7, self.COLORS['white'], -1)
                cv2.circle(frame, keypoint_coords["left_shoulder"], 7, self.COLORS['yellow'], -1)
                cv2.circle(frame, keypoint_coords["right_shoulder"], 7, self.COLORS['magenta'], -1)
                if display_inactivity or (time.time() - self.state_tracker['INACTIVE_TIME_START']) <= 3:
                    cv2.putText(frame, 'Resetting CURLS due to inactivity!', (10, 90),
                                self.font, 0.5, self.COLORS['red'], 2, lineType=self.linetype)
                    play_sound = 'reset_counters'
                    self.state_tracker['INACTIVE_TIME_FRONT'] = 0.0
                    self.state_tracker['start_inactive_time_front'] = time.perf_counter()
                    if not (time.time() - self.state_tracker['INACTIVE_TIME_START']) <= 3:
                        self.state_tracker['INACTIVE_TIME_START'] = time.time()
                # self.draw_curls_counter(frame, curls)
                # self.cv_elem.draw_text(
                #     frame,
                #     'CAMERA NOT ALIGNED PROPERLY!!!',
                #     pos=(30, 60),
                #     text_color=(255, 255, 255),
                #     font_scale=0.65,
                #     text_color_bg=(255, 153, 0),
                # )
                # self.cv_elem.draw_text(
                #     frame,
                #     'OFFSET ANGLE: ' + str(angles["offset_angle"]),
                #     pos=(30, 30),
                #     text_color=(255, 255, 255),
                #     font_scale=0.65,
                #     text_color_bg=(255, 153, 0),
                # )
                # Reset inactive times for side view.
                self.state_tracker['start_inactive_time'] = time.perf_counter()
                self.state_tracker['INACTIVE_TIME'] = 0.0
                self.state_tracker['prev_state'] = None
                self.state_tracker['curr_state'] = None
                pass
            else:
                self.state_tracker['INACTIVE_TIME_FRONT'] = 0.0
                self.state_tracker['start_inactive_time_front'] = time.perf_counter()
                # Determine dominant side
                dominant_side = self.get_dominant_side(keypoint_coords)

                # Draw angle visualizations (ellipses and lines) ONLY FOR DOMINANT SIDE
                for angle_data in self.angles:
                    if "vertical" in angle_data and angle_data["vertical"]:
                        point1 = f"{dominant_side}_{angle_data['point1']}" if not angle_data['point1'].startswith(
                            ('left_', 'right_')) else angle_data['point1']
                        point2 = f"{dominant_side}_{angle_data['point2']}" if not angle_data['point2'].startswith(
                            ('left_', 'right_')) else angle_data['point2']
                        p1 = keypoint_coords[point1]
                        p2 = keypoint_coords[point2]
                        angle_value = angles[angle_data["name"]]

                        # Определение направления дуги и начального угла
                        if angle_data["direction"] == "up":
                            start_angle = -90
                            if dominant_side == "left":
                                if angle_data['point1'] == "ankle":
                                    end_angle = start_angle - angle_value
                                elif angle_data['point1'] == "knee":
                                    end_angle = start_angle + angle_value
                                elif angle_data['point1'] == "hip":
                                    end_angle = start_angle - angle_value
                                else:
                                    end_angle = start_angle + angle_value
                            else:  # dominant_side == "right"
                                if angle_data['point1'] == "ankle":
                                    end_angle = start_angle + angle_value
                                elif angle_data['point1'] == "knee":
                                    end_angle = start_angle - angle_value
                                elif angle_data['point1'] == "hip":
                                    end_angle = start_angle + angle_value
                                else:
                                    end_angle = start_angle - angle_value
                        else:  # angle_data["direction"] == "down"
                            start_angle = 90
                            if dominant_side == "left":
                                if angle_data['point1'] == "knee":
                                    end_angle = start_angle + angle_value
                                elif angle_data['point1'] == "hip":
                                    end_angle = start_angle - angle_value
                                elif angle_data['point1'] == "shoulder":
                                    end_angle = start_angle + angle_value
                                else:
                                    end_angle = start_angle - angle_value
                            else:  # dominant_side == "right"
                                if angle_data['point1'] == "knee":
                                    end_angle = start_angle - angle_value
                                elif angle_data['point1'] == "hip":
                                    end_angle = start_angle + angle_value
                                elif angle_data['point1'] == "shoulder":
                                    end_angle = start_angle - angle_value
                                else:
                                    end_angle = start_angle + angle_value

                        cv2.ellipse(frame, p1, (30, 30),
                                    angle=0, startAngle=start_angle, endAngle=end_angle,
                                    color=self.COLORS['white'], thickness=3, lineType=self.linetype)

                        # Отрисовка вертикальной линии выше или ниже точки
                        line_start = (p1[0], p1[1] - 80) if angle_data["direction"] == "up" else (p1[0], p1[1] + 80)
                        line_end = (p1[0], p1[1] + 20) if angle_data["direction"] == "up" else (p1[0], p1[1] - 20)
                        if line_start[1] > line_end[1]:
                            line_end, line_start = (int(line_start[0]), int(line_start[1])), (
                                int(line_end[0]), int(line_end[1]))
                        self.cv_elem.draw_dotted_line(frame, p1, start=line_start[1], end=line_end[1],
                                                      line_color=self.COLORS['purple'])

                        cv2.putText(frame, str(int(angle_value)), (p1[0] + 10, p1[1]), self.font, 0.6,
                                    self.COLORS['light_green'], 2, lineType=self.linetype)
                    elif angle_data["name"] != "offset_angle":
                        #  Отрисовка обычного угла
                        point1 = f"{dominant_side}_{angle_data['point1']}" if not angle_data['point1'].startswith(
                            ('left_', 'right_')) else angle_data['point1']
                        point2 = f"{dominant_side}_{angle_data['point2']}" if not angle_data['point2'].startswith(
                            ('left_', 'right_')) else angle_data['point2']
                        #  Исправление:  обрабатываем  точку  "nose"  отдельно
                        ref_point = angle_data['ref_point'] if angle_data[
                                                                   'ref_point'] == "nose" else f"{dominant_side}_{angle_data['ref_point']}" if not \
                            angle_data['ref_point'].startswith(('left_', 'right_')) else angle_data['ref_point']

                        p1 = keypoint_coords[point1]
                        p2 = keypoint_coords[point2]
                        ref_pt = keypoint_coords[ref_point]
                        angle_value = angles[angle_data["name"]]

                        #  Отрисовка эллипса
                        cv2.ellipse(frame, ref_pt, (30, 30),
                                    angle=angle_value, startAngle=0, endAngle=0,
                                    color=self.COLORS['white'], thickness=3, lineType=self.linetype)

                        #  Отрисовка  текста  со  значением  угла
                        cv2.putText(frame, str(int(angle_value)), (ref_pt[0] + 10, ref_pt[1]), self.font, 0.6,
                                    self.COLORS['light_green'], 2, lineType=self.linetype)
                        # --- Plotting landmarks if it wasn't implemented in the detector
                #
                if not self.detector.is_plotted:
                    # plotting landmarks ONLY FOR DOMINANT SIDE AND ONLY FOR POINTS IN JSON
                    for keypoint_type in self.landmark_features_dict[dominant_side]:
                        keypoint_name = f"{dominant_side}_{keypoint_type}"
                        if keypoint_name in keypoint_coords:
                            # Рисуем точку
                            cv2.circle(frame, keypoint_coords[keypoint_name], 7, self.COLORS['white'], -1,
                                       lineType=self.linetype)
                            # Рисуем линию к предыдущей точке (если она есть)
                            if keypoint_type == "hip":  # Соединяем hip с shoulder
                                prev_keypoint_name = f"{dominant_side}_shoulder"
                                self.draw_landmark_line(frame, keypoint_coords[prev_keypoint_name],
                                                        keypoint_coords[keypoint_name], self.COLORS['pink'], 4)
                            elif keypoint_type != self.landmark_features_dict[dominant_side][
                                0]:  # Пропускаем первую точку
                                prev_keypoint_name = f"{dominant_side}_{self.landmark_features_dict[dominant_side][self.landmark_features_dict[dominant_side].index(keypoint_type) - 1]}"
                                self.draw_landmark_line(frame, keypoint_coords[prev_keypoint_name],
                                                        keypoint_coords[keypoint_name], self.COLORS['pink'], 4)
                # Get current state and update state sequence
                self.current_state = current_state = self.get_current_state(angles)
                self.state_tracker['curr_state'] = current_state
                self._update_state_sequence(current_state)
                print(f"\rcurr_state: {self.state_tracker['curr_state']}\tstate_seq: {self.state_tracker['state_seq']}",
                      end='')

                # Check thresholds and draw feedback
                has_critical_error = self.check_thresholds_and_draw_feedback(frame, angles, keypoint_coords)
                if not self.state_tracker['CRITICAL_ERROR']:
                    self.state_tracker['CRITICAL_ERROR'] = has_critical_error

                    # Check for incorrect sequence
                    expected_state_index = len(self.state_tracker['state_seq']) - 1
                    #  Получаем  success_sequence  для  текущего  уровня  сложности
                    success_sequence = \
                        self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"][
                            "success_sequence"]
                    if expected_state_index >= 0 and expected_state_index < len(success_sequence):
                        expected_state = success_sequence[expected_state_index]
                        incorrect_sequence = current_state != expected_state
                    else:
                        incorrect_sequence = False
                # Count reps
                if current_state == \
                        self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"][
                            "success_sequence"][0]:  # Возвращение в начальное состояние
                    if len(self.state_tracker['state_seq']) in [
                        len(self.state_machine["beginner" if self.thresholds is self.thresholds_beginner else "pro"][
                                "success_sequence"]) - i for i in range(2)]:

                        if not self.state_tracker['INCORRECT_POSTURE'] and not self.state_tracker[
                            'CRITICAL_ERROR'] and not incorrect_sequence:
                            self.state_tracker['CURLS'] += 1
                            play_sound = str(self.state_tracker['CURLS'])
                        else:
                            # Увеличиваем BAD_CURLS только один раз за неверное повторение
                            self.state_tracker['BAD_CURLS'] += 1
                            play_sound = 'incorrect'
                        # Очищаем state_seq и сбрасываем INCORRECT_POSTURE
                        self.state_tracker['state_seq'] = []
                        self.state_tracker['INCORRECT_POSTURE'] = False
                        self.state_tracker['CRITICAL_ERROR'] = False

                # Handle inactivity
                display_inactivity = self.check_inactivity()
                # Draw feedback messages
                # self.state_tracker['COUNT_FRAMES'][self.state_tracker['DISPLAY_TEXT']] += 1
                # frame = self._show_feedback(frame, feedback_indices, keypoint_coords)
                if display_inactivity or (time.time() - self.state_tracker['INACTIVE_TIME_START']) <= 3:
                    cv2.putText(frame, 'Resetting CURLS due to inactivity!', (10, 90),
                                self.font, 0.5, self.COLORS['red'], 2, lineType=self.linetype)
                    play_sound = 'reset_counters'
                    self.state_tracker['INACTIVE_TIME_FRONT'] = 0.0
                    self.state_tracker['start_inactive_time_front'] = time.perf_counter()
                    if not (time.time() - self.state_tracker['INACTIVE_TIME_START']) <= 3:
                        self.state_tracker['INACTIVE_TIME_START'] = time.time()
                # Display angles and rep counts
                self.draw_curls_counter(frame, curls)
                self.state_tracker['DISPLAY_TEXT'][
                    self.state_tracker['COUNT_FRAMES'] > self.thresholds['CNT_FRAME_THRESH']] = False
                self.state_tracker['COUNT_FRAMES'][
                    self.state_tracker['COUNT_FRAMES'] > self.thresholds['CNT_FRAME_THRESH']] = 0
                self.state_tracker['prev_state'] = current_state
        else:
            # ... Handle no keypoints detected (similar to existing code)
            end_time = time.perf_counter()
            self.state_tracker['INACTIVE_TIME'] += end_time - self.state_tracker['start_inactive_time']
            display_inactivity = False
            if self.state_tracker['INACTIVE_TIME'] >= self.thresholds['INACTIVE_THRESH'] or (
                    time.time() - self.state_tracker['INACTIVE_TIME_START']) <= 3:
                self.state_tracker['CURLS'] = 0
                self.state_tracker['BAD_CURLS'] = 0
                cv2.putText(frame, 'Resetting CURLS due to inactivity!!!', (10, frame_height - 25), self.font, 0.7,
                            self.COLORS['red'], 2)
                display_inactivity = True
                if not (time.time() - self.state_tracker['INACTIVE_TIME_START']) <= 3:
                    self.state_tracker['INACTIVE_TIME_START'] = time.time()
            self.state_tracker['start_inactive_time'] = end_time
            self.draw_curls_counter(frame, curls)
            if display_inactivity or (time.time() - self.state_tracker['INACTIVE_TIME_START']) <= 3:
                cv2.putText(frame, 'Resetting CURLS due to inactivity!', (10, 90),
                            self.font, 0.5, self.COLORS['red'], 2, lineType=self.linetype)
                play_sound = 'reset_counters'
                self.state_tracker['INACTIVE_TIME_FRONT'] = 0.0
                self.state_tracker['start_inactive_time_front'] = time.perf_counter()
                if not (time.time() - self.state_tracker['INACTIVE_TIME_START']) <= 3:
                    self.state_tracker['INACTIVE_TIME_START'] = time.time()
            # Reset all other state variables
            self.state_tracker['prev_state'] = None
            self.state_tracker['curr_state'] = None
            self.state_tracker['INACTIVE_TIME_FRONT'] = 0.0
            self.state_tracker['INCORRECT_POSTURE'] = False
            self.state_tracker['DISPLAY_TEXT'] = np.full((5,), False)
            self.state_tracker['COUNT_FRAMES'] = np.zeros((5,), dtype=np.int64)
            self.state_tracker['start_inactive_time_front'] = time.perf_counter()
            pass
        # Show FPS
        if show_fps:
            cTime = time.time()
            fps = 1 / (cTime - pTime)
            pTime = cTime
            cv2.putText(frame, f'fps: {int(fps)}', (1180, 45), cv2.FONT_HERSHEY_PLAIN, 1.2,
                        (255, 255, 255), 2)
        return frame, play_sound
