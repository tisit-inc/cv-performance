# app/services/feedback_manager.py
from typing import List, Dict, Any
from app.utils.logger import setup_logger
from app.services.redis_buffer import RedisBuffer
from app.services.llm_analyzer import LLMAnalyzer


class FeedbackManager:
    # TODO: move necessary params to config
    def __init__(self,
                 redis_buffer: RedisBuffer,
                 window_size: float = 2.5,
                 min_frame_ratio: float = 0.6,
                 llm_analyzer: LLMAnalyzer | None = None
                 ):
        self.redis_buffer = redis_buffer
        self.WINDOW_SIZE = window_size
        self.MIN_FRAME_RATIO = min_frame_ratio
        self.llm_analyzer = llm_analyzer
        self.logger = setup_logger("feedback_manager")

    async def process_frame_feedback(self, session_uuid: str, timestamp: float, feedback_ids: List[str]) -> None:
        """Обработка фидбека для отдельного кадра"""
        try:
            await self.redis_buffer.add_frame_feedback(session_uuid, timestamp, feedback_ids)
            await self._process_active_window(session_uuid, timestamp, feedback_ids)
        except Exception as e:
            self.logger.error(f"Error processing frame feedback: {e}")
            raise

    async def _process_active_window(self, session_uuid: str, timestamp: float, feedback_ids: List[str]) -> None:
        """Обработка активного окна фидбека"""
        window = await self._get_or_create_active_window(session_uuid, timestamp)

        if timestamp - window['start_time'] >= self.WINDOW_SIZE:
            await self._validate_and_save_window(session_uuid, window)
            await self._create_new_window(session_uuid, timestamp)
        else:
            await self._update_window_stats(session_uuid, window, feedback_ids)

    async def _get_or_create_active_window(self, session_uuid: str, timestamp: float) -> Dict[str, Any]:
        """Получение или создание активного окна"""
        window = await self.redis_buffer.get_active_window(session_uuid)
        if not window:
            window = {
                'start_time': timestamp,
                'end_time': timestamp + self.WINDOW_SIZE,
                'feedback_stats': {}
            }
            await self.redis_buffer.save_active_window(session_uuid, window)
        return window

    async def _validate_and_save_window(self, session_uuid: str, window: Dict[str, Any]) -> None:
        """Валидация и сохранение окна фидбека"""
        total_frames = sum(stat['count'] for stat in window['feedback_stats'].values())

        if total_frames > 0:
            valid_feedback = {
                feedback_id: {
                    'count': stats['count'],
                    'frame_ratio': stats['count'] / total_frames
                }
                for feedback_id, stats in window['feedback_stats'].items()
                if stats['count'] / total_frames >= self.MIN_FRAME_RATIO
            }

            window_data = {
                'start_time': window['start_time'],
                'end_time': window['end_time'],
                'feedback_stats': valid_feedback
            }

            await self.redis_buffer.save_window(session_uuid, window_data)

    async def _update_window_stats(self, session_uuid: str, window: Dict[str, Any], feedback_ids: List[str]) -> None:
        """Обновление статистики активного окна"""
        for feedback_id in feedback_ids:
            if feedback_id not in window['feedback_stats']:
                window['feedback_stats'][feedback_id] = {'count': 0}
            window['feedback_stats'][feedback_id]['count'] += 1

        await self.redis_buffer.save_active_window(session_uuid, window)

    async def _create_new_window(self, session_uuid: str, timestamp: float) -> None:
        """Создание нового активного окна"""
        new_window = {
            'start_time': timestamp,
            'end_time': timestamp + self.WINDOW_SIZE,
            'feedback_stats': {}
        }
        await self.redis_buffer.save_active_window(session_uuid, new_window)

    async def get_exercise_summary(self, session_uuid: str) -> Dict[str, Any]:
        """Получение итогового отчета по фидбеку упражнения"""
        windows = await self.redis_buffer.get_feedback_windows(session_uuid)
        current_state = await self.redis_buffer.get_current_state(
            session_uuid)  # TODO: think about necessity of this data

        if not windows:
            return {"windows": [], "feedback_summary": {}}

        feedback_frequency = {}
        total_windows = len(windows)

        for window in windows:
            for feedback_id, stats in window['feedback_stats'].items():
                if feedback_id not in feedback_frequency:
                    feedback_frequency[feedback_id] = {'windows_count': 0, 'total_occurrences': 0}
                if stats['frame_ratio'] >= self.MIN_FRAME_RATIO:
                    feedback_frequency[feedback_id]['windows_count'] += 1
                feedback_frequency[feedback_id]['total_occurrences'] += stats['count']

        return {
            "windows": windows,
            "feedback_summary": {
                feedback_id: {
                    **stats,
                    "window_ratio": stats['windows_count'] / total_windows
                }
                for feedback_id, stats in feedback_frequency.items()
            },
            "exercise_duration": windows[-1]['end_time'] - windows[0]['start_time'],
            "exercise_performance": current_state['curls']
        }

        # we can actually add
        # # forming summary
        # summary = {
        #     'main_issues': [],  # Основные проблемы (встречались часто)
        #     'occasional_issues': [],  # Периодические проблемы
        #     'statistics': feedback_frequency
        # }
        #
        # # classify problems
        # for feedback_id, stats in feedback_frequency.items():
        #     window_ratio = stats['windows_count'] / total_windows
        #     if window_ratio >= 0.7:  # Проблема присутствовала в большинстве окон
        #         summary['main_issues'].append(feedback_id)
        #     elif window_ratio >= 0.3:  # Периодически возникающая проблема
        #         summary['occasional_issues'].append(feedback_id)
        #
        # return summary

    async def analyze_exercise(self, session_uuid: str, language: str = 'ru') -> Dict[str, Any]:
        """Analysis of the exercise feedback with LLM"""
        exercise_summary = await self.get_exercise_summary(session_uuid)

        result = {"summary": exercise_summary}

        if self.llm_analyzer:
            llm_analysis = await self.llm_analyzer.analyze_exercise(exercise_summary, language)
            result["llm_analysis"] = llm_analysis

        return result
