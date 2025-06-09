import threading
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.kafka_handlers.topic_manager import TopicManager
from app.services.universal_quality_processor import UniversalQualityProcessor
from app.utils.logger import setup_logger

logger = setup_logger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("📊 API Server component initialized")
    yield
    logger.info("🛑 API Server component shutdown")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {
        "service": "tisit-performance-svc",
        "version": "3.0",
        "description": "Universal Quality Assessment System",
        "status": "running"
    }


@app.get("/ping")
async def ping():
    return {"message": "pong"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "tisit-performance-svc v3.0",
        "components": {
            "api_server": "running",
            "universal_processor": "running",
            "kafka_integration": "active",
            "semantic_taxonomy": "loaded"
        }
    }


@app.get("/exercises")
async def list_exercises():
    """List available exercise configurations"""
    from app.cv.metrics_storage import MetricsStorage
    from app.core.config import get_settings
    
    settings = get_settings()
    storage = MetricsStorage(settings.metrics_dir)
    
    available_exercises = storage.list_available_exercises_v2()
    
    return {
        "available_exercises": available_exercises,
        "v2_semantic_configs": [name for name, version in available_exercises.items() if version == "2.0"],
        "legacy_configs": [name for name, version in available_exercises.items() if version == "1.0"]
    }


@app.get("/config/{exercise_name}")
async def get_exercise_config(exercise_name: str):
    """Get configuration for a specific exercise"""
    from app.cv.metrics_storage import MetricsStorage
    from app.core.config import get_settings
    
    settings = get_settings()
    storage = MetricsStorage(settings.metrics_dir)
    
    try:
        # Try v2.0 first
        config = storage.get_metrics_for_exercise_v2(f"{exercise_name}-v2.json")
        return {
            "exercise": exercise_name,
            "version": "2.0",
            "config": config
        }
    except FileNotFoundError:
        try:
            # Fallback to legacy
            config = storage.get_metrics_for_exercise(exercise_name)
            return {
                "exercise": exercise_name,
                "version": "1.0", 
                "config": config
            }
        except Exception as e:
            return {
                "error": f"Exercise configuration not found: {exercise_name}",
                "details": str(e)
            }


def run_fastapi_server():
    """Run FastAPI server in separate thread"""
    import uvicorn
    try:
        uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
    except Exception as e:
        logger.error(f"FastAPI server error: {e}")


def main():
    """Main entry point - runs both API server and Universal Processor"""
    logger.info(
        """Starting tisit-performance-svc v3.0 🚀
        ╔═══════════════════════════════════════════════════════════╗
        ║              tisit-performance-svc v3.0                   ║
        ║         Universal Quality Assessment System               ║
        ║    API Server + Universal Quality Processor              ║
        ╚═══════════════════════════════════════════════════════════╝"""
    )

    # Initialize Kafka topics
    topic_manager = TopicManager()
    topic_manager.ensure_topics_exist()
    topic_manager.describe_topics()

    # Start FastAPI server in separate thread
    api_thread = threading.Thread(target=run_fastapi_server, daemon=True)
    api_thread.start()
    logger.info("🌐 API Server started on http://0.0.0.0:8001")
    
    # Give API server time to start
    time.sleep(2)
    
    # Start Universal Quality Processor in main thread (for signal handlers)
    logger.info("🎯 Starting Universal Quality Processor in main thread...")
    try:
        universal_processor = UniversalQualityProcessor()
        universal_processor.run()  # This blocks in main thread
    except KeyboardInterrupt:
        logger.info("🛑 Shutdown initiated by user")
    except Exception as e:
        logger.error(f"💥 Universal processor error: {e}")
    finally:
        logger.info("✅ Application shutdown complete")


if __name__ == "__main__":
    main()