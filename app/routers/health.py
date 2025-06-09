from typing import Dict, Any
import asyncio

from fastapi import APIRouter, status

from app.core.config import get_settings
from app.utils.logger import setup_logger

router = APIRouter(prefix="/health", tags=["health"])
logger = setup_logger(__name__)


async def health_check() -> Dict[str, Any]:
    """Perform comprehensive health check"""
    checks = {}
    settings = get_settings()
    
    # Check Quix Streams connectivity (simplified check)
    try:
        # Simple check - verify Kafka bootstrap servers are reachable
        import socket
        host, port = settings.kafka_bootstrap_servers.split(':')[0], int(settings.kafka_bootstrap_servers.split(':')[1])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        checks["kafka"] = result == 0
        logger.debug("Kafka connectivity check passed")
    except Exception as e:
        checks["kafka"] = False
        logger.warning("Kafka connectivity check failed: %s", e)
    
    # Check if Universal Quality Processor is responsive
    # (This is a simplified check - in real scenario you'd check processor state)
    try:
        # Check if we can load exercise configurations
        from app.cv.metrics_storage import MetricsStorage
        storage = MetricsStorage(settings.metrics_dir)
        available_exercises = storage.list_available_exercises_v2()
        checks["exercise_configs"] = len(available_exercises) > 0
        logger.debug("Exercise configurations check passed: %d configs", len(available_exercises))
    except Exception as e:
        checks["exercise_configs"] = False
        logger.warning("Exercise configurations check failed: %s", e)
    
    # Check Quix Streams state directory (if exists)
    try:
        import pathlib
        state_dir = pathlib.Path("app/state")
        if state_dir.exists():
            # Check if state directory is accessible
            list(state_dir.iterdir())
            checks["quix_state"] = True
        else:
            # State directory doesn't exist yet - that's OK for new installations
            checks["quix_state"] = True
        logger.debug("Quix Streams state check passed")
    except Exception as e:
        checks["quix_state"] = False
        logger.warning("Quix Streams state check failed: %s", e)
    
    # Overall status
    status_result = "ok" if all(v for v in checks.values()) else "degraded"
    
    return {
        "status": status_result,
        "checks": checks,
        "service": "tisit-performance-svc",
        "version": settings.RELEASE_VER
    }


@router.get("", status_code=status.HTTP_200_OK)
async def health_check_simple() -> bool:
    """Simple health check - returns True if all systems are operational"""
    result = await health_check()
    return result.get("status") == "ok"


@router.get("-detailed", status_code=status.HTTP_200_OK)
async def health_check_detailed() -> Dict[str, Any]:
    """Detailed health check with component status breakdown"""
    return await health_check()


@router.get("/kafka", status_code=status.HTTP_200_OK)
async def health_check_kafka() -> Dict[str, Any]:
    """Kafka connectivity check (simplified for Quix Streams)"""
    settings = get_settings()
    
    try:
        # Simple connectivity check
        import socket
        host, port = settings.kafka_bootstrap_servers.split(':')[0], int(settings.kafka_bootstrap_servers.split(':')[1])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            return {
                "status": "ok",
                "bootstrap_servers": settings.kafka_bootstrap_servers,
                "connectivity": "reachable",
                "note": "Using Quix Streams - detailed topic info not available"
            }
        else:
            return {
                "status": "error",
                "error": f"Cannot connect to {settings.kafka_bootstrap_servers}",
                "connectivity": "unreachable"
            }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "connectivity": "unknown"
        }


@router.get("/exercises", status_code=status.HTTP_200_OK)
async def health_check_exercises() -> Dict[str, Any]:
    """Exercise configurations health check"""
    settings = get_settings()
    
    try:
        from app.cv.metrics_storage import MetricsStorage
        storage = MetricsStorage(settings.metrics_dir)
        
        available_exercises = storage.list_available_exercises_v2()
        v2_configs = [name for name, version in available_exercises.items() if version == "2.0"]
        
        return {
            "status": "ok",
            "total_configs": len(available_exercises),
            "v2_configs": len(v2_configs),
            "available_exercises": list(available_exercises.keys()),
            "semantic_ready": v2_configs
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "total_configs": 0,
            "available_exercises": []
        }