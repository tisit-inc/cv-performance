import asyncio
from collections import defaultdict
from datetime import datetime, timedelta

from app.cv.exercise_processor import ExerciseProcessor
from app.cv.metrics_storage import MetricsStorage
from app.kafka_handlers.consumer import KafkaConsumer
from app.services.feedback_manager import FeedbackManager
from app.services.redis_buffer import RedisBuffer

# TODO: implement KafkaProducerService
# from app.kafka_handlers.producer import KafkaProducer
from app.core.config import get_settings
from app.utils.logger import setup_logger


class PerformanceService:
    # TODO: add workers logic for processing frames after implementing the services
    def __init__(self):
        self.settings = get_settings()
        self.logger = setup_logger("performance_service")
        self.consumer = KafkaConsumer()
        self.redis_buffer = RedisBuffer(self.settings.redis_url, "production")
        self.metrics_storage = MetricsStorage(self.settings.metrics_dir)
        self.feedback_manager = FeedbackManager(self.redis_buffer)

        # self.producer = KafkaProducerService()

        self.active_sessions = defaultdict(lambda: {
            'last_active': datetime.now(),
            'is_completing': False  # флаг завершения
        })
        self.session_timeout = timedelta(minutes=5)  # Сохраняем таймаут сессии
        self.processing_interval = 1.0  # Сохраняем интервал обработки

    async def initialize(self):
        await self.redis_buffer.connect()
        self.logger.info("PerformanceService initialized")

    async def process_message(self, message):
        try:
            self.logger.info(f"Processing message for frame: {message.frame_track_uuid}")
            await self.redis_buffer.add_frame(message.session_uuid, message.model_dump())
            self.logger.debug(f"Message for frame: {message.frame_track_uuid} added to buffer")
            self.active_sessions[message.session_uuid].update({
                'last_active': datetime.now()
            })  # Обновляем время последней активности сессии
            self.logger.debug(f"Message for frame: {message.frame_track_uuid} added to queue")
        except Exception as e:
            self.logger.error(f"Unexpected error in process_message: {e}", exc_info=True)

    # TODO: rewrite and optimize this crunch code (from while True to asyncio.gather)
    async def process_active_sessions(self):
        while True:
            try:
                current_time = datetime.now()
                sessions_to_remove = []

                # --- this is info for logging
                total_frames_processed = 0
                active_sessions_count = len(self.active_sessions)
                # ---

                if active_sessions_count > 0:
                    self.logger.info(f"Checking {active_sessions_count} active sessions")

                for session_uuid, session_data in list(self.active_sessions.items()):
                    if current_time - session_data['last_active'] > self.session_timeout:
                        sessions_to_remove.append(session_uuid)
                        continue
                    try:
                        frames = await self.redis_buffer.get_frames_for_processing(session_uuid)
                        if frames:
                            self.logger.info(f"Processing {len(frames)} frames from session {session_uuid}")
                            exercise_name = frames[0].get('exercise')

                            exercise_metrics = self.metrics_storage.get_metrics_for_exercise(exercise_name)
                            self.logger.info(f"using metrics for exercise: {exercise_name}")

                            processor = ExerciseProcessor(
                                exercise_metrics=exercise_metrics,
                                redis_buffer=self.redis_buffer,
                                feedback_manager=self.feedback_manager
                            )

                            for frame in frames:
                                await processor.process_frame(session_uuid, frame)
                                self.logger.debug(
                                    f"Processed frame {frame['frame_track_uuid']} for session {session_uuid}")
                                total_frames_processed += 1

                                # Set session as completing if the last frame is reached
                                if frame['current_time'] >= frame['video_duration'] - 0.1:
                                    session_data['is_completing'] = True


                            if session_data['is_completing']:
                                final_results = {
                                    'state': await self.redis_buffer.get_current_state(session_uuid),
                                    'feedback': await self.feedback_manager.get_exercise_summary(session_uuid),
                                    'session_uuid': session_uuid,
                                    'exercise': frames[-1]['exercise'],
                                    'timestamp': frames[-1]['timestamp']
                                }
                                self.logger.debug(f"Sending final results for session {session_uuid}: {final_results}")
                                # await self._send_final_results(final_results)
                                sessions_to_remove.append(session_uuid)
                            else:
                                # Обновляем время последней активности только для незавершенных сессий
                                session_data['last_active'] = current_time

                    except Exception as e:
                        self.logger.error(f"Error processing session {session_uuid}: {e}", exc_info=True)

                for session_uuid in sessions_to_remove:
                    # Для сессий с таймаутом отправляем финальный отчет, если он еще не был отправлен
                    if session_uuid in self.active_sessions and not self.active_sessions[session_uuid]['is_completing']:
                        final_results = {
                            'state': await self.redis_buffer.get_current_state(session_uuid),
                            'feedback': await self.feedback_manager.get_exercise_summary(session_uuid),
                            'session_uuid': session_uuid,
                            'status': 'timeout'
                        }
                        # await self._send_final_results(final_results)

                    del self.active_sessions[session_uuid]
                    self.logger.info(f"Removed inactive session: {session_uuid}")

                if total_frames_processed > 0:
                    self.logger.info(
                        f"Processed {total_frames_processed} frames across {active_sessions_count} active sessions")
                else:
                    self.logger.debug("No frames processed in this iteration")

            except Exception as e:
                self.logger.error(f"Unexpected error in process_active_sessions: {e}", exc_info=True)

            await asyncio.sleep(self.processing_interval)

    async def run(self):
        await self.initialize()
        self.logger.info("Starting PerformanceService...")

        process_sessions_task = asyncio.create_task(self.process_active_sessions())

        try:
            async for message in self.consumer.run():
                if message:
                    self.logger.info(f"Received message for frame: {message.frame_track_uuid}")
                    await self.process_message(message)
        except asyncio.CancelledError:
            self.logger.info("PerformanceService cancelled")
        finally:
            process_sessions_task.cancel()
            try:
                await process_sessions_task
            except asyncio.CancelledError:
                pass
            await self.close()

    async def close(self):
        self.logger.info("Closing PerformanceService")
        await self.consumer.close()
        await self.redis_buffer.disconnect()
        # await self.producer.close()
        self.logger.info("PerformanceService stopped")


if __name__ == "__main__":
    service = PerformanceService()
    asyncio.run(service.run())
