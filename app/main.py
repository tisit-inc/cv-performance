import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.kafka_handlers.topic_manager import TopicManager
from app.services.performance_service import PerformanceService
from app.utils.logger import setup_logger

logger = setup_logger("main")


# TODO: also look for the possibility of using signal handlers


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        """Starting tisit-performance-svc
        ╔═════════════════════════════════════╗
        ║        tisit-performance-svc        ║
        ╚═════════════════════════════════════╝"""
    )

    # topic_manager init
    topic_manager = TopicManager()
    topic_manager.ensure_topics_exist()
    topic_manager.describe_topics()

    # TODO: look for the possibility of creating pool for the PerformanceService()
    performance_service = PerformanceService()
    performance_service_task = asyncio.create_task(performance_service.run())
    yield

    logger.info("Shutting down application")
    performance_service_task.cancel()

    try:
        # TODO: use config for timeout
        await asyncio.wait_for(performance_service_task, timeout=30.0)
    except asyncio.TimeoutError:
        logger.warning("Performance service did not complete within the timeout period")
    except asyncio.CancelledError:
        logger.info("Performance service was cancelled successfully")

    await performance_service.close()
    logger.info("Application shutdown complete")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "CV Performance Service is running"}


@app.get("/ping")
async def ping():
    return {"message": "pong"}


# TODO: add host and port to config
if __name__ == "__main__":
    import uvicorn

    try:
        uvicorn.run(app, host="0.0.0.0", port=8001)
    except KeyboardInterrupt:
        logger.info("Server shutdown initiated by user")
