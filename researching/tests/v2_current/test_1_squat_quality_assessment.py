#!/usr/bin/env python3
"""
🧪 Тест Squat Quality Assessment System

Отправляет тестовые CV данные для приседаний в Kafka и проверяет работу системы
"""

import json
import asyncio
import time
from datetime import datetime
from confluent_kafka import Producer, Consumer
from typing import Dict, Any

# Тестовые CV данные для приседаний
SAMPLE_SQUAT_FRAMES = [
    {
        "frame_track_uuid": "test_squat_001",
        "timestamp": datetime.now().isoformat(),
        "exercise": "squat",
        "session_uuid": "test_squat_session_001",
        "video_duration": 30.0,
        "current_time": 1.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "knee_angle", "value": 170.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # s1 - standing
                {"name": "hip_angle", "value": 175.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "back_angle", "value": 25.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_squat_002", 
        "timestamp": datetime.now().isoformat(),
        "exercise": "squat",
        "session_uuid": "test_squat_session_001",
        "video_duration": 30.0,
        "current_time": 2.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "knee_angle", "value": 130.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # s2 - descending
                {"name": "hip_angle", "value": 110.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "back_angle", "value": 30.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_squat_003",
        "timestamp": datetime.now().isoformat(), 
        "exercise": "squat",
        "session_uuid": "test_squat_session_001",
        "video_duration": 30.0,
        "current_time": 3.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "knee_angle", "value": 75.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # s3 - deep squat
                {"name": "hip_angle", "value": 125.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "back_angle", "value": 35.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_squat_004_DANGEROUS",
        "timestamp": datetime.now().isoformat(), 
        "exercise": "squat",
        "session_uuid": "test_squat_session_001",
        "video_duration": 30.0,
        "current_time": 4.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "knee_angle", "value": 40.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # КРИТИЧЕСКАЯ SAFETY ОШИБКА!
                {"name": "hip_angle", "value": 160.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},  # Плохая мобильность
                {"name": "back_angle", "value": 60.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}  # КРИТИЧЕСКИЙ НАКЛОН ВПЕРЕД!
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    }
]

class SquatSystemTester:
    def __init__(self):
        # Kafka конфигурация
        self.producer_config = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'squat_test_producer'
        }
        
        self.consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'squat_test_consumer_group_' + str(int(time.time())),
            'auto.offset.reset': 'latest',
            'client.id': 'squat_test_consumer'
        }
        
        self.input_topic = "performance-input"
        self.output_topic = "performance-feedback"
        
        try:
            self.producer = Producer(self.producer_config)
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe([self.output_topic])
            print("✅ Kafka connection established")
        except Exception as e:
            print(f"❌ Kafka connection failed: {e}")
            print("💡 Убедитесь что Kafka запущен на localhost:9092")
            raise

    def send_test_frames(self):
        """Отправка тестовых кадров приседаний в Kafka"""
        print("🚀 Отправка тестовых CV данных для squat...")
        
        for i, frame in enumerate(SAMPLE_SQUAT_FRAMES):
            # Обновляем timestamp для реалистичности
            frame["timestamp"] = datetime.now().isoformat()
            
            message = json.dumps(frame)
            
            self.producer.produce(
                topic=self.input_topic,
                key=frame["session_uuid"],
                value=message
            )
            
            knee_angle = frame['metrics']['angles'][0]['value']
            if knee_angle > 160:
                state = "s1 (standing)"
            elif knee_angle > 90:
                state = "s2 (descending)"
            else:
                state = "s3 (deep squat)"
            
            print(f"📤 Отправлен кадр {i+1}: knee_angle={knee_angle}, state={state}")
            time.sleep(1)  # Пауза между кадрами
        
        self.producer.flush()
        print("✅ Все тестовые кадры отправлены")

    def listen_for_results(self, timeout_seconds=30):
        """Прослушивание результатов от Universal Processor"""
        print(f"👂 Ожидание результатов от Universal Processor (timeout: {timeout_seconds}s)...")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            msg = self.consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                print(f"❌ Kafka error: {msg.error()}")
                continue
            
            try:
                result = json.loads(msg.value().decode('utf-8'))
                self.print_coaching_result(result)
                
            except Exception as e:
                print(f"❌ Error parsing result: {e}")
        
        print(f"⏰ Timeout reached ({timeout_seconds}s)")

    def print_coaching_result(self, result: Dict[str, Any]):
        """Красивый вывод результата coaching для squat"""
        print("\n" + "="*60)
        print("🏋️ SQUAT COACHING RESULT RECEIVED")
        print("="*60)
        
        print(f"📋 Session: {result.get('session_id', 'unknown')}")
        print(f"🦵 Exercise: {result.get('exercise_type', 'unknown')}")
        print(f"⏰ Timestamp: {result.get('analysis_timestamp', 'unknown')}")
        
        # Семантический контекст
        semantic_context = result.get('semantic_context', {})
        print(f"\n📊 Analysis Window: {semantic_context.get('analysis_window_ms', 0)}ms")
        print(f"🔄 Movement Flow: {semantic_context.get('movement_flow', [])}")
        print(f"⚠️  Error Patterns: {semantic_context.get('error_patterns_detected', 0)}")
        
        # Coaching focus areas
        focus_areas = result.get('coaching_focus_areas', {})
        
        safety = focus_areas.get('safety_priorities', [])
        if safety:
            print(f"\n🚨 SAFETY PRIORITIES ({len(safety)}):")
            for issue in safety:
                print(f"   • {issue.get('error_code', 'unknown')}: {issue.get('description', 'no description')}")
                print(f"     Frequency: {issue.get('frequency', 0)}, Priority: {issue.get('coaching_priority', 'unknown')}")
        
        technical = focus_areas.get('technique_improvements', [])
        if technical:
            print(f"\n🎯 TECHNIQUE IMPROVEMENTS ({len(technical)}):")
            for issue in technical:
                print(f"   • {issue.get('error_code', 'unknown')}: {issue.get('description', 'no description')}")
                print(f"     Frequency: {issue.get('frequency', 0)}, Priority: {issue.get('coaching_priority', 'unknown')}")
        
        form = focus_areas.get('form_corrections', [])
        if form:
            print(f"\n💪 FORM CORRECTIONS ({len(form)}):")
            for issue in form:
                print(f"   • {issue.get('error_code', 'unknown')}: {issue.get('description', 'no description')}")
                print(f"     Frequency: {issue.get('frequency', 0)}, Priority: {issue.get('coaching_priority', 'unknown')}")
        
        execution = focus_areas.get('execution_guidance', [])
        if execution:
            print(f"\n⚡ EXECUTION GUIDANCE ({len(execution)}):")
            for issue in execution:
                print(f"   • {issue.get('error_code', 'unknown')}: {issue.get('description', 'no description')}")
                print(f"     Frequency: {issue.get('frequency', 0)}, Priority: {issue.get('coaching_priority', 'unknown')}")
        
        # LLM processing hints
        hints = result.get('llm_processing_hints', {})
        print(f"\n🤖 LLM PROCESSING HINTS:")
        print(f"   Urgency: {hints.get('urgency_level', 'unknown')}")
        print(f"   Complexity: {hints.get('coaching_complexity', 0)}")
        print(f"   Session Maturity: {hints.get('session_maturity', 0)} windows")
        print(f"   Intervention History: {hints.get('intervention_history', 0)} times")
        
        print("="*60)

    def run_full_test(self):
        """Полный тест системы для squat"""
        print("""
🧪 SQUAT QUALITY ASSESSMENT SYSTEM TEST
======================================

Этот тест:
1. Отправляет 4 кадра с данными приседаний
2. Кадр 1: s1 (standing) - норма
3. Кадр 2: s2 (descending) - норма  
4. Кадр 3: s3 (deep squat) - норма
5. Кадр 4: КРИТИЧЕСКИЕ SAFETY ОШИБКИ (knee valgus + forward lean)
6. Ожидает coaching results от Universal Processor

Убедитесь что:
- Kafka запущен на localhost:9092
- tisit-performance-svc запущен (python -m app.main)
- squat-v2.json конфигурация создана
""")
        
        input("Нажмите Enter для начала теста...")
        
        # Отправка тестовых данных
        self.send_test_frames()
        
        print("\n⏳ Ожидание обработки (Universal Processor создает окна по 3 секунды)...")
        time.sleep(2)  # Даем время на обработку
        
        # Прослушивание результатов
        self.listen_for_results(timeout_seconds=30)
        
        print("\n✅ Тест завершен")


if __name__ == "__main__":
    print("🎯 Тестирование squat упражнения")
    
    tester = SquatSystemTester()
    tester.run_full_test()