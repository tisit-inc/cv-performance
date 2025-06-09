# app/services/redis_buffer.py
import redis.asyncio as redis
import json
from datetime import datetime
from typing import Dict, List, Any  # todo: use python 3.10 | instead of typing.List, typing.Dict and so on
from app.utils.logger import setup_logger


# TODO: rename to RedisService
class RedisBuffer:
    # TODO: move necessary params to config
    def __init__(self, redis_url: str, environment: str = "dev",
                 max_buffer_size: int = 30, processing_window: int = 10):
        self.redis_url = redis_url
        self.env = environment
        self.redis = None
        self.logger = setup_logger("redis_buffer")
        self.max_buffer_size = max_buffer_size  # in sec
        self.processing_window = processing_window  # in sec

    async def connect(self):
        """Connect to Redis by url"""
        try:
            self.redis = await redis.from_url(self.redis_url)
            self.logger.info(f"Successfully connected to Redis at {self.redis_url}")
        except redis.RedisError as e:
            self.logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            raise

    async def disconnect(self):
        """Disconnect from Redis if connected"""
        if self.redis:
            try:
                await self.redis.close()
                self.logger.info("Disconnected from Redis")
            except redis.RedisError as e:
                self.logger.error(f"Error disconnecting from Redis: {e}", exc_info=True)
            finally:
                self.redis = None

    @staticmethod
    def datetime_serializer(obj):
        # Serialize datetime objects to ISO format
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    @staticmethod
    def datetime_deserializer(dct):
        # Deserialize datetime strings to datetime objects
        for key, value in dct.items():
            if isinstance(value, str):
                try:
                    dct[key] = datetime.fromisoformat(value)
                except ValueError:
                    pass
        return dct

    async def add_frame(self, session_uuid: str, frame_data: Dict[str, Any]) -> None:
        """Add frame data to Redis sorted set and remove old frames by using TTL (zremrangebyscore)"""
        key = f"cv:perf:{self.env}:frame:{session_uuid}"
        try:
            timestamp = int(frame_data["timestamp"].timestamp() * 1000)
            frame_json = json.dumps(frame_data, default=self.datetime_serializer)

            async with self.redis.pipeline(transaction=True) as pipe:
                await pipe.zadd(key, {frame_json: timestamp})
                await pipe.zremrangebyscore(key, 0, int(timestamp - self.max_buffer_size * 1000))  # remove old frames
                await pipe.expire(key, self.max_buffer_size * 200)  # TTL: 2x size of max_buffer_size
                # TODO: change TTL
                await pipe.execute()
            self.logger.debug(f"Added frame to Redis for session {session_uuid}")
        except (redis.RedisError, KeyError, TypeError) as e:
            self.logger.error(f"Error adding frame to Redis: {e}", exc_info=True)
            raise

    # TODO: remove excess logging
    async def get_frames_for_processing(self, session_uuid: str) -> List[Dict[str, Any]]:
        """Get frames from Redis sorted set for processing and remove them from the set"""
        key = f"cv:perf:{self.env}:frame:{session_uuid}"
        last_processed_key = f"cv:perf:{self.env}:last_processed:{session_uuid}"
        try:
            current_time = int(datetime.now().timestamp() * 1000)
            last_processed_time = await self.redis.get(last_processed_key)

            last_processed_time = int(last_processed_time) if last_processed_time else 0

            start_time = last_processed_time + 1
            end_time = current_time - int(self.processing_window * 1000)

            self.logger.debug(f"Fetching frames for session {session_uuid} from {start_time} to {end_time}")
            self.logger.info(f"Last processed time: {last_processed_time}")

            frames = await self.redis.zrangebyscore(key, start_time, end_time)

            if frames:
                self.logger.info(f"Retrieved {len(frames)} frames for processing from session {session_uuid}")
                processed_frames = [json.loads(frame, object_hook=self.datetime_deserializer) for frame in frames]

                # обновляем время последнего обработанного кадра
                await self.redis.set(last_processed_key, end_time,
                                     ex=self.max_buffer_size * 10)  # TTL: 10x size of max_buffer_size (is this ok?)

                # удаляем обработанные кадры из буфера
                removed = await self.redis.zremrangebyscore(key, start_time, end_time)
                self.logger.debug(f"Removed {removed} processed frames for session {session_uuid}")

                return processed_frames
            else:
                self.logger.debug(f"No frames found for processing in session {session_uuid}")
            return []

        except (redis.RedisError, json.JSONDecodeError) as e:
            self.logger.error(f"Error retrieving frames for processing from Redis: {e}", exc_info=True)
            raise

    # async def clean_old_frames(self, session_uuid: str, max_age_seconds: int) -> None:
    #     key = f"cv:perf:{self.env}:frame:{session_uuid}"
    #     try:
    #         max_timestamp = int((datetime.now().timestamp() - max_age_seconds) * 1000)
    #         removed = await self.redis.zremrangebyscore(key, 0, max_timestamp)
    #         self.logger.info(f"Removed {removed} old frames for session {session_uuid}")
    #     except redis.RedisError as e:
    #         self.logger.error(f"Error cleaning old frames from Redis: {e}", exc_info=True)
    #         raise

    # unused right now
    async def get_active_sessions(self) -> List[str]:
        try:
            pattern = f"cv:perf:{self.env}:frame:*"
            keys = await self.redis.keys(pattern)
            sessions = [key.split(':')[-1] for key in keys]
            return sessions
        except redis.RedisError as e:
            self.logger.error(f"Error retrieving active sessions from Redis: {e}", exc_info=True)
            raise

    # unused right now
    async def get_latest_frame(self, session_uuid: str) -> Dict[str, Any] | None:
        key = f"cv:perf:{self.env}:frame:{session_uuid}"
        try:
            latest_frame = await self.redis.zrange(key, -1, -1)
            if latest_frame:
                self.logger.debug(f"Retrieved latest frame for session {session_uuid}")
                return json.loads(latest_frame[0], object_hook=self.datetime_deserializer)
            else:
                self.logger.warning(f"No frames found for session {session_uuid}")
                return None
        except (redis.RedisError, json.JSONDecodeError) as e:
            self.logger.error(f"Error retrieving latest frame from Redis: {e}", exc_info=True)
            raise

    # # method for performance tracking
    # async def update_state(self, session_uuid: str, exercise: str, timestamp: datetime,
    #                        current_state: str = None) -> None:
    #     key = f"cv:perf:{self.env}:state:{session_uuid}"
    #     try:
    #         state_data = {
    #             "exercise": exercise,
    #             "last_processed_timestamp": int(timestamp.timestamp() * 1000),
    #         }
    #         if current_state:
    #             state_data["current_state"] = current_state
    #         await self.redis.hmset(key, state_data)
    #         self.logger.debug(f"Updated state for session {session_uuid}")
    #     except redis.RedisError as e:
    #         self.logger.error(f"Error updating state in Redis: {e}", exc_info=True)
    #         raise
    #
    # # method for performance tracking
    # async def get_state(self, session_uuid: str) -> Dict[str, Any]:
    #     key = f"cv:perf:{self.env}:state:{session_uuid}"
    #     try:
    #         state = await self.redis.hgetall(key)
    #         if state:
    #             self.logger.debug(f"Retrieved state for session {session_uuid}")
    #             return {k.decode(): v.decode() for k, v in state.items()}
    #         else:
    #             self.logger.warning(f"No state found for session {session_uuid}")
    #             return {}
    #     except redis.RedisError as e:
    #         self.logger.error(f"Error retrieving state from Redis: {e}", exc_info=True)
    #         raise

    async def get_current_state(self, session_uuid: str) -> dict:
        """Getting current state for session from Redis"""
        key = f"cv:perf:{self.env}:state:{session_uuid}"
        try:
            state = await self.redis.get(key)
            if state:
                return json.loads(state, object_hook=self.datetime_deserializer)
            else:
                return self._create_initial_state()
        except (redis.RedisError, json.JSONDecodeError) as e:
            self.logger.error(f"Error retrieving current state from Redis: {e}", exc_info=True)
            raise

    async def save_state(self, session_uuid: str, state: dict) -> None:
        """Save current state for session in Redis"""
        key = f"cv:perf:{self.env}:state:{session_uuid}"
        try:
            state_json = json.dumps(state, default=self.datetime_serializer)
            # TODO: max_buffer_size for state is 2x of max_buffer_size for frames -> need to be tested
            await self.redis.set(key, state_json, ex=self.max_buffer_size * 2)  # TTL: 2x size of max_buffer_size
            self.logger.debug(f"Saved state to Redis for session {session_uuid}")
        except (redis.RedisError, TypeError) as e:
            self.logger.error(f"Error saving state to Redis: {e}", exc_info=True)
            raise

    @staticmethod
    def _create_initial_state() -> dict:
        """Creating initial state for session"""
        return {
            'curr_state': 's1',
            'prev_state': None,
            'state_seq': [],
            'curls': {
                'correct': 0,
                'incorrect': 0
            },
            'timers': {
                'inactive_time': 0.0,
                'inactive_time_front': 0.0,
            },
            'flags': {
                'incorrect_posture': False,
                'critical_error': False
            }
        }

    # async def update_state(self,
    #                        session_uuid: str,
    #                        new_state: str,
    #                        is_correct: bool = True,
    #                        flags: dict | None = None) -> None:
    #     """Update current state for session in Redis"""
    #     try:
    #         current = await self.get_current_state(session_uuid)
    #
    #         # Обновляем состояния
    #         current['prev_state'] = current['curr_state']
    #         current['curr_state'] = new_state
    #
    #         # Обновляем последовательность состояний
    #         if new_state != current['curr_state']:
    #             current['state_seq'].append(new_state)
    #
    #         # Проверяем завершение повторения
    #         if self._is_rep_completed(current['state_seq']):
    #             if is_correct:
    #                 current['curls']['correct'] += 1
    #             else:
    #                 current['curls']['incorrect'] += 1
    #             current['state_seq'] = []  # Сбрасываем последовательность
    #
    #         # Обновляем флаги
    #         if flags:
    #             current['flags'].update(flags)
    #
    #         # Сохраняем обновленное состояние
    #         await self.redis.set(
    #             f"exercise:{session_uuid}:current_state",
    #             json.dumps(current)
    #         )
    #         self.logger.debug(f"Updated state for session {session_uuid}: {current}")
    #     except redis.RedisError as e:
    #         self.logger.error(f"Error updating state: {e}", exc_info=True)
    #         raise
    #
    # async def update_timers(self,
    #                         session_uuid: str,
    #                         inactive_time: float | None = None,
    #                         inactive_time_front: float | None = None) -> None:
    #     """Обновление таймеров неактивности"""
    #     try:
    #         current = await self.get_current_state(session_uuid)
    #
    #         if inactive_time is not None:
    #             current['timers']['inactive_time'] = inactive_time
    #         if inactive_time_front is not None:
    #             current['timers']['inactive_time_front'] = inactive_time_front
    #
    #         await self.redis.set(
    #             f"exercise:{session_uuid}:current_state",
    #             json.dumps(current)
    #         )
    #         self.logger.debug(f"Updated timers for session {session_uuid}")
    #     except redis.RedisError as e:
    #         self.logger.error(f"Error updating timers: {e}", exc_info=True)
    #         raise

    # TODO: add methods for feedback processing

    async def add_frame_feedback(self, session_uuid: str, timestamp: float, feedback_ids: List[str]) -> None:
        """Add frame feedback to Redis sorted set"""
        key = f"cv:perf:{self.env}:feedback:{session_uuid}:frames"
        try:
            frame_feedback = {
                'timestamp': timestamp,
                'feedback_ids': feedback_ids
            }
            await self.redis.zadd(key, {json.dumps(frame_feedback): timestamp})
            await self.redis.expire(key, self.max_buffer_size * 2)
        except redis.RedisError as e:
            self.logger.error(f"Error adding frame feedback to Redis: {e}", exc_info=True)
            raise

    async def get_active_window(self, session_uuid: str) -> Dict[str, Any]:
        """Get active feedback window from Redis"""
        key = f"cv:perf:{self.env}:feedback:{session_uuid}:active_window"
        try:
            window = await self.redis.get(key)
            return json.loads(window) if window else None
        except redis.RedisError as e:
            self.logger.error(f"Error getting active window from Redis: {e}", exc_info=True)
            raise

    async def save_active_window(self, session_uuid: str, window: Dict[str, Any]) -> None:
        """Save active feedback window to Redis"""
        key = f"cv:perf:{self.env}:feedback:{session_uuid}:active_window"
        try:
            await self.redis.set(key, json.dumps(window), ex=self.max_buffer_size * 2)
        except redis.RedisError as e:
            self.logger.error(f"Error saving active window to Redis: {e}", exc_info=True)
            raise

    async def save_window(self, session_uuid: str, window: Dict[str, Any]) -> None:
        """Save feedback window to Redis history"""
        key = f"cv:perf:{self.env}:feedback:{session_uuid}:windows"
        try:
            await self.redis.zadd(key, {json.dumps(window): window['end_time']})
            await self.redis.expire(key, self.max_buffer_size * 2)
        except redis.RedisError as e:
            self.logger.error(f"Error saving window to Redis: {e}", exc_info=True)
            raise

    async def get_feedback_windows(self, session_uuid: str) -> List[Dict[str, Any]]:
        """Get feedback windows from Redis history"""
        key = f"cv:perf:{self.env}:feedback:{session_uuid}:windows"
        try:
            windows = await self.redis.zrange(key, 0, -1, withscores=True)
            return [json.loads(w) for w, _ in windows]
        except redis.RedisError as e:
            self.logger.error(f"Error getting feedback windows from Redis: {e}", exc_info=True)
            raise
