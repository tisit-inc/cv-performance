from app.services.feedback_manager import FeedbackManager
from app.services.redis_buffer import RedisBuffer
from datetime import datetime


# TODO: work with comments
class ExerciseProcessor:
    def __init__(
            self,
            exercise_metrics: dict,  # already loaded metrics
            redis_buffer: RedisBuffer,
            feedback_manager: FeedbackManager,  # TODO: на каком уровне мы все-таки работаем с feedback_manager?
            level: int = 0
    ):
        # exercise metrics
        self.exercise_metrics = exercise_metrics
        self.thresholds = exercise_metrics['thresholds']['pro' if level else 'beginner']
        self.state_machine = exercise_metrics['state_machine']['pro' if level else 'beginner']

        # services
        self.redis_buffer = redis_buffer
        self.feedback_manager = feedback_manager

    def set_level(self, level: int = 0) -> None:
        """Установка уровня сложности и соответствующих порогов"""
        self.thresholds = self.exercise_metrics['thresholds']['pro' if level else 'beginner']

    async def process_frame(self, session_uuid: str, frame_data: dict):
        """Processing frame with metrics"""

        # getting angles from input data
        angles = {
            angle['name']: angle['value']
            for angle in frame_data['metrics']['angles']
        }

        # Проверяем смещение и получаем текущее состояние
        is_incorrect_posture = self._check_offset_angle(angles)
        current_state = await self.redis_buffer.get_current_state(
            session_uuid)  # TODO: насколько корректно тут вызывать buffer? Может делать это в performance_service?

        if is_incorrect_posture:
            await self._handle_incorrect_posture(session_uuid, current_state, frame_data['timestamp'])
            return

        # Определяем новое состояние и обновляем его
        new_state = self._get_current_state(angles)
        updated_state = self._update_state(current_state, new_state)
        await self.redis_buffer.save_state(session_uuid, updated_state)

        # Проверяем пороговые значения и генерируем фидбек
        feedback_ids = self._check_thresholds(angles, new_state)
        await self.feedback_manager.process_frame_feedback(
            session_uuid,
            frame_data['timestamp'],
            feedback_ids
        )

    def _check_offset_angle(self, angles: dict[str, float]) -> bool:
        threshold = self.thresholds['OFFSET_THRESH']
        angle_name = next((angle["name"] for angle in self.exercise_metrics['angles']
                           if "offset" in angle["name"]), None)
        return angle_name and abs(angles[angle_name]) > threshold

    async def _handle_incorrect_posture(self, session_uuid: str, current_state: dict, timestamp: float):
        current_state['flags']['incorrect_posture'] = True
        current_state['timers']['inactive_time_front'] += timestamp - current_state.get('last_timestamp', timestamp)

        if current_state['timers']['inactive_time_front'] >= self.thresholds['INACTIVE_THRESH']:
            current_state['curls'] = {'correct': 0, 'incorrect': 0}
            current_state['timers']['inactive_time_front'] = 0.0

        current_state['last_timestamp'] = timestamp
        await self.redis_buffer.save_state(session_uuid, current_state)

    def _get_current_state(self, angles: dict[str, float]) -> str | None:
        """Определение текущего состояния на основе углов"""
        states = self.state_machine["states"]
        for state in states:
            if eval(state["condition"], {"__builtins__": None}, angles):
                return state["id"]
        return None

    def _update_state(self, current_state: dict, new_state: str) -> dict:
        """
        Update exercise state

        Args:
            current_state: Current state from Redis
            new_state: New state based on angles
        """
        success_sequence = self.state_machine["success_sequence"]

        # Обновляем последовательность состояний
        if new_state in success_sequence:
            if len(current_state['state_seq']) == 0 or new_state != current_state['state_seq'][-1]:
                current_state['state_seq'].append(new_state)

                # Ограничиваем длину последовательности
                if len(current_state['state_seq']) > len(self.state_machine['success_sequence']):
                    current_state['state_seq'] = current_state['state_seq'][1:]

        # Проверяем завершение повторения
        if (new_state == success_sequence[0] and
                len(current_state['state_seq']) == len(success_sequence)):

            # Определяем правильность повторения
            is_correct = not (current_state['flags']['incorrect_posture'] or
                              current_state['flags']['critical_error'])

            # Обновляем счетчики
            if is_correct:
                current_state['curls']['correct'] += 1
            else:
                current_state['curls']['incorrect'] += 1

            # Сбрасываем флаги и последовательность
            current_state['state_seq'] = []
            current_state['flags']['incorrect_posture'] = False
            current_state['flags']['critical_error'] = False

        # Обновляем состояния и проверяем неактивность
        if current_state['curr_state'] == new_state:
            current_time = datetime.now().timestamp()
            current_state['timers']['inactive_time'] += current_time - current_state.get('last_timestamp', current_time)

            if current_state['timers']['inactive_time'] >= self.thresholds['INACTIVE_THRESH']:
                current_state['curls'] = {'correct': 0, 'incorrect': 0}
                current_state['timers']['inactive_time'] = 0.0
        else:
            current_state['timers']['inactive_time'] = 0.0

        current_state['prev_state'] = current_state['curr_state']
        current_state['curr_state'] = new_state
        current_state['last_timestamp'] = datetime.now().timestamp()

        return current_state

    def _check_thresholds(self, angles: dict[str, float], current_state: str):
        """
        Проверка пороговых значений углов и генерация фидбека

        Args:
            angles: Словарь углов и их значений
            current_state: Текущее состояние упражнения

        Returns:
            List[str]: Список идентификаторов активного фидбека
        """
        feedback_ids = []

        # Проверяем пороги только если не в начальном состоянии
        if current_state != 's1':
            for angle_name, thresholds in self.thresholds["angle_thresh"].items():
                if angle_name not in angles:
                    continue

                angle_value = angles[angle_name]
                for threshold in thresholds:
                    if ((threshold["min"] is not None and angle_value < threshold["min"]) or
                            (threshold["max"] is not None and angle_value > threshold["max"])):

                        feedback_ids.append(self.exercise_metrics['feedback_messages'][threshold['feedback']]['id'])

                        # Если ошибка критическая, помечаем это в состоянии
                        if threshold.get("critical"):
                            return feedback_ids, True

        return feedback_ids
