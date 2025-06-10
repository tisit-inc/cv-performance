#!/usr/bin/env python3
"""
Standalone processor runner for debugging Universal Quality Processor v3.0
Runs the processor without FastAPI server for pure stream processing testing.
"""

import sys
import os
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from app.services.performance_service import PerformanceService
from app.utils.logger import setup_logger


def main():
    """Run Universal Quality Processor in standalone debug mode"""
    
    # Setup enhanced logging for debugging
    logger = setup_logger(__name__, level=logging.DEBUG)
    
    logger.info("=" * 60)
    logger.info("🔧 DEBUG MODE: Performance Service v3.0")
    logger.info("=" * 60)
    logger.info("Running processor standalone without API server")
    logger.info("Press Ctrl+C to stop")
    logger.info("-" * 60)
    
    try:
        # Initialize and run performance service
        processor = PerformanceService()
        logger.info("✅ Processor initialized successfully")
        logger.info("🚀 Starting stream processing...")
        
        # This will block and process messages
        processor.run()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Shutdown initiated by user (Ctrl+C)")
    except Exception as e:
        logger.error("❌ Processor error: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        logger.info("🛑 Standalone processor shutdown complete")


if __name__ == "__main__":
    main()