#!/usr/bin/env python3
"""
🌉 Bridge CV-to-Performance Service

Мост между CV сервисом и Performance сервисом:
- Читает результаты CV из 'inference_results' 
- Отправляет в 'performance-input' для Universal Processor
- Трансформирует формат данных если нужно
"""

import json
import time
from datetime import datetime
from confluent_kafka import Consumer, Producer
from app.utils.logger import setup_logger

logger = setup_logger("cv_performance_bridge")


class CVPerformanceBridge:
    def __init__(self):
        self.bootstrap_servers = "localhost:9092"
        
        # CV results topic (input)
        self.cv_topic = "inference_results"
        
        # Performance input topic (output) 
        self.performance_topic = "performance-input"
        
        # Kafka setup
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'cv-performance-bridge',
            'auto.offset.reset': 'latest'
        })
        
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap_servers
        })
        
        self.consumer.subscribe([self.cv_topic])
        logger.info(f"🌉 Bridge initialized: {self.cv_topic} → {self.performance_topic}")

    def transform_cv_to_performance(self, cv_result: dict) -> dict:
        """Трансформация CV результата в формат для Performance сервиса"""
        try:
            # CV результат уже в нужном формате InferenceOutput
            # Просто проверим ключевые поля
            required_fields = ['frame_track_uuid', 'timestamp', 'exercise', 'session_uuid', 'metrics']
            
            for field in required_fields:
                if field not in cv_result:
                    logger.warning(f"⚠️ Missing field {field} in CV result")
                    return None
            
            # Добавим недостающие поля если нужно
            performance_data = cv_result.copy()
            
            # Ensure video_duration and current_time exist
            if 'video_duration' not in performance_data:
                performance_data['video_duration'] = 60.0  # Default
            
            if 'current_time' not in performance_data:
                # Use timestamp or current time
                performance_data['current_time'] = time.time()
            
            logger.debug(f"✅ Transformed CV result for frame {cv_result['frame_track_uuid']}")
            return performance_data
            
        except Exception as e:
            logger.error(f"❌ Transform error: {e}")
            return None

    def run(self):
        """Основной цикл моста"""
        logger.info("🚀 Starting CV-Performance Bridge...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    logger.error(f"❌ Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Parse CV result
                    cv_data = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"📥 Received CV result: {cv_data.get('frame_track_uuid', 'unknown')}")
                    
                    # Transform for Performance service
                    performance_data = self.transform_cv_to_performance(cv_data)
                    
                    if performance_data:
                        # Send to Performance topic
                        message = json.dumps(performance_data, default=str)
                        self.producer.produce(
                            topic=self.performance_topic,
                            key=performance_data['session_uuid'],
                            value=message
                        )
                        
                        logger.info(f"📤 Forwarded to Performance: {performance_data['frame_track_uuid']}")
                        
                        # Show key metrics for debugging
                        angles = performance_data.get('metrics', {}).get('angles', [])
                        if angles:
                            elbow_angle = next((a['value'] for a in angles if a['name'] == 'elbow_angle'), None)
                            if elbow_angle:
                                logger.debug(f"🔄 Elbow angle: {elbow_angle}° → FSM state detection")
                    
                    self.producer.poll(0)  # Trigger delivery
                    
                except Exception as e:
                    logger.error(f"❌ Message processing error: {e}")
                    
        except KeyboardInterrupt:
            logger.info("🛑 Bridge shutdown initiated by user")
        finally:
            self.consumer.close()
            self.producer.flush()
            logger.info("✅ Bridge shutdown complete")


if __name__ == "__main__":
    bridge = CVPerformanceBridge()
    bridge.run()