#!/usr/bin/env python3
"""
🚀 Standalone запуск Universal Quality Processor

Этот скрипт запускает только процессор без FastAPI,
избегая проблем с signal handlers в background threads.
"""

import asyncio
import signal
import sys
from app.services.universal_quality_processor import UniversalQualityProcessor
from app.kafka_handlers.topic_manager import TopicManager
from app.utils.logger import setup_logger

logger = setup_logger("standalone_processor")


class StandaloneProcessor:
    def __init__(self):
        self.processor = None
        self.running = True
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def run(self):
        """Run the standalone processor"""
        logger.info(
            """🚀 Starting Universal Quality Processor (Standalone Mode)
            ╔═══════════════════════════════════════════════════════════╗
            ║          Universal Quality Assessment Processor           ║
            ║                    Standalone Mode                        ║
            ║           Semantic Error Taxonomy + Coaching             ║
            ╚═══════════════════════════════════════════════════════════╝"""
        )
        
        # Setup signal handlers
        self.setup_signal_handlers()
        
        try:
            # Initialize Kafka topics
            logger.info("📋 Initializing Kafka topics...")
            topic_manager = TopicManager()
            topic_manager.ensure_topics_exist()
            topic_manager.describe_topics()
            
            # Create and run processor
            logger.info("🎯 Creating Universal Quality Processor...")
            self.processor = UniversalQualityProcessor()
            
            logger.info("🌟 Starting processor in main thread...")
            logger.info("📊 System ready to process exercise quality assessment")
            logger.info("💡 Press Ctrl+C to stop")
            
            # Run processor (this blocks)
            self.processor.run()
            
        except KeyboardInterrupt:
            logger.info("🛑 Received keyboard interrupt")
        except Exception as e:
            logger.error(f"💥 Processor error: {e}", exc_info=True)
        finally:
            logger.info("✅ Standalone processor shutdown complete")


if __name__ == "__main__":
    standalone = StandaloneProcessor()
    standalone.run()