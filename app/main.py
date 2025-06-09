import traceback
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.routers.health import router as health_router
from app.services.universal_quality_processor import UniversalQualityProcessor
from app.utils.logger import setup_logger


async def startup_tasks():
    """Initialize application components"""
    app.include_router(health_router)

    # Note: Topic management is now handled by Quix Streams internally
    logger.info(
        "Using Quix Streams for topic management - no manual topic creation needed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        """Starting tisit-performance-svc
        ╔══════════════════════════════╗
        ║    tisit-performance-svc     ║
        ╚══════════════════════════════╝"""
    )

    # Execute startup tasks
    await startup_tasks()

    logger.info("API Server component initialized")

    yield

    logger.info("API Server component shutdown")


settings = get_settings()
logger = setup_logger(__name__)


@dataclass(frozen=True)
class AppMeta:
    title: str = "API: Performance Service"
    commit: str = f"commit: {settings.GIT_COMMIT[:7]}"
    build_time: str = f"build time: {settings.BUILD_TIME_MOSCOW}"

    @property
    def metadata(self) -> str:
        return f"({self.commit}, {self.build_time})"


meta = AppMeta()

app = FastAPI(
    docs_url=None,
    openapi_url=None,
    redoc_url=None,
    lifespan=lifespan,
    title=meta.title,
    version=settings.RELEASE_VER
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
    logger.error("Validation error for %s: %s", request.url.path, exc_str)
    return JSONResponse(content={'message': exc_str}, status_code=422)


@app.exception_handler(Exception)
async def exception_callback(request: Request, exc: Exception):
    # Handle exception groups for Python 3.11+ compatibility
    if hasattr(exc, 'exceptions') and hasattr(exc, '__iter__'):
        exc = exc.exceptions[0] if exc.exceptions else exc

    logger.error("Unhandled exception for %s: %s",
                 request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "err_type": str(repr(exc)),
            "trace": traceback.format_exc() if settings.is_development else "hidden"
        },
    )


@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": settings.PROJECT_NAME,
        "version": settings.RELEASE_VER,
        "description": "Universal Exercise Quality Assessment System",
        "status": "running",
        "architecture": "v3.0"
    }


@app.get("/ping", include_in_schema=False)
async def ping():
    """Simple ping endpoint for load balancer health checks"""
    return {
        "msg": "pong",
        "commit": settings.GIT_COMMIT,
        "build_time": settings.BUILD_TIME_MOSCOW,
        "service": settings.PROJECT_NAME
    }


@app.get("/exercises")
async def list_exercises():
    """List available exercise configurations"""
    from app.cv.metrics_storage import MetricsStorage

    storage = MetricsStorage(settings.metrics_dir)
    available_exercises = storage.list_available_exercises_v2()

    return {
        "available_exercises": available_exercises,
        "v2_semantic_configs": [name for name, version in available_exercises.items() if version == "2.0"],
        "legacy_configs": [name for name, version in available_exercises.items() if version == "1.0"],
        "total_count": len(available_exercises)
    }


@app.get("/config/{exercise_name}")
async def get_exercise_config(exercise_name: str):
    """Get configuration for a specific exercise"""
    from app.cv.metrics_storage import MetricsStorage

    storage = MetricsStorage(settings.metrics_dir)

    try:
        # Try v2.0 first
        config = storage.get_metrics_for_exercise_v2(
            f"{exercise_name}-v2.json")
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
            return JSONResponse(
                status_code=404,
                content={
                    "error": f"Exercise configuration not found: {exercise_name}",
                    "details": str(e)
                }
            )


@app.get("/docs", include_in_schema=False)
async def get_documentation():
    """API documentation"""
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title=meta.title + " " + meta.metadata
    )


@app.get("/openapi.json", include_in_schema=False)
async def openapi():
    """OpenAPI schema"""
    return get_openapi(
        title=meta.title,
        version=settings.RELEASE_VER + " " + meta.metadata,
        routes=app.routes
    )


def run_fastapi_server():
    """Run FastAPI server in separate thread"""
    import uvicorn
    try:
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            log_level="info",
            access_log=not settings.is_production
        )
    except Exception as e:
        logger.error("FastAPI server error: %s", e)


def main():
    """Main entry point - Universal Quality Processor as primary worker with API server"""
    logger.info(
        "Starting tisit-performance-svc v3.0 - Universal Quality Assessment System")

    # Start FastAPI server in background thread (API is secondary)
    api_thread = threading.Thread(target=run_fastapi_server, daemon=True)
    api_thread.start()
    logger.info("API Server started on http://0.0.0.0:8000")

    # Give API server time to start
    time.sleep(2)

    # Start Universal Quality Processor in main thread (primary worker)
    logger.info("Starting Universal Quality Processor in main thread")
    try:
        universal_processor = UniversalQualityProcessor()
        universal_processor.run()  # This blocks in main thread
    except KeyboardInterrupt:
        logger.info("Shutdown initiated by user")
    except Exception as e:
        logger.error("Universal processor error: %s", e, exc_info=True)
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()
