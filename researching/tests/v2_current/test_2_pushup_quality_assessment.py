#!/usr/bin/env python3
"""
🧪 Тест Push-up Quality Assessment System

Отправляет тестовые CV данные для отжиманий в Kafka и проверяет работу системы
"""

import json
import time
from datetime import datetime
from confluent_kafka import Producer, Consumer
from typing import Dict, Any

# Тестовые CV данные для отжиманий
SAMPLE_PUSHUP_FRAMES = [
    {
        "frame_track_uuid": "test_pushup_001",
        "timestamp": datetime.now().isoformat(),
        "exercise": "push-up",
        "session_uuid": "test_pushup_session_001",
        "video_duration": 30.0,
        "current_time": 1.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "elbow_angle", "value": 175.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # s1 - up position
                {"name": "shoulder_angle", "value": 95.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "body_angle", "value": 180.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}  # Perfect plank
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_pushup_002", 
        "timestamp": datetime.now().isoformat(),
        "exercise": "push-up",
        "session_uuid": "test_pushup_session_001",
        "video_duration": 30.0,
        "current_time": 2.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "elbow_angle", "value": 120.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # s2 - descending
                {"name": "shoulder_angle", "value": 110.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "body_angle", "value": 175.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}  # Good form
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_pushup_003",
        "timestamp": datetime.now().isoformat(), 
        "exercise": "push-up",
        "session_uuid": "test_pushup_session_001",
        "video_duration": 30.0,
        "current_time": 3.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "elbow_angle", "value": 80.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # s3 - bottom position
                {"name": "shoulder_angle", "value": 130.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "body_angle", "value": 170.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}  # Still good
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_pushup_004_BAD_FORM",
        "timestamp": datetime.now().isoformat(), 
        "exercise": "push-up",
        "session_uuid": "test_pushup_session_001",
        "video_duration": 30.0,
        "current_time": 4.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "elbow_angle", "value": 130.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # Shallow push-up
                {"name": "shoulder_angle", "value": 85.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "body_angle", "value": 130.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}  # КРИТИЧЕСКОЕ ПРОВИСАНИЕ!
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_pushup_005_OVEREXTENSION",
        "timestamp": datetime.now().isoformat(), 
        "exercise": "push-up",
        "session_uuid": "test_pushup_session_001",
        "video_duration": 30.0,
        "current_time": 5.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "elbow_angle", "value": 195.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # ОПАСНОЕ ПЕРЕРАЗГИБАНИЕ!
                {"name": "shoulder_angle", "value": 75.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "body_angle", "value": 220.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}  # КРИТИЧЕСКОЕ ПОДНЯТИЕ ТАЗА!
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    }
]

class PushupSystemTester:
    def __init__(self):
        # Kafka конфигурация
        self.producer_config = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'pushup_test_producer'
        }
        
        self.consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'pushup_test_consumer_group_' + str(int(time.time())),
            'auto.offset.reset': 'latest',
            'client.id': 'pushup_test_consumer'
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
        """Отправка тестовых кадров отжиманий в Kafka"""
        print("🚀 Отправка тестовых CV данных для push-up...")
        
        for i, frame in enumerate(SAMPLE_PUSHUP_FRAMES):
            # Обновляем timestamp для реалистичности
            frame["timestamp"] = datetime.now().isoformat()
            
            message = json.dumps(frame)
            
            self.producer.produce(
                topic=self.input_topic,
                key=frame["session_uuid"],
                value=message
            )
            
            elbow_angle = frame['metrics']['angles'][0]['value']
            body_angle = frame['metrics']['angles'][2]['value']
            
            if elbow_angle > 160:
                state = "s1 (up)"
            elif elbow_angle > 90:
                state = "s2 (descending)" 
            else:
                state = "s3 (bottom)"
            
            safety_note = ""
            if body_angle < 140:
                safety_note = " 🚨 DANGEROUS SAGGING!"
            elif body_angle > 200:
                safety_note = " 🚨 DANGEROUS PIKING!"
            elif elbow_angle > 190:
                safety_note = " 🚨 ARM OVEREXTENSION!"
            
            print(f"📤 Кадр {i+1}: elbow={elbow_angle:.0f}°, body={body_angle:.0f}°, state={state}{safety_note}")
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
        """Красивый вывод результата coaching для push-up"""
        print("\n" + "="*60)
        print("💪 PUSH-UP COACHING RESULT RECEIVED")
        print("="*60)
        
        print(f"📋 Session: {result.get('session_id', 'unknown')}")
        print(f"🔥 Exercise: {result.get('exercise_type', 'unknown')}")
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
        """Полный тест системы для push-up"""
        print("""
🧪 PUSH-UP QUALITY ASSESSMENT SYSTEM TEST
=========================================

Этот тест:
1. Отправляет 5 кадров с данными отжиманий
2. Кадр 1: s1 (up position) - норма
3. Кадр 2: s2 (descending) - норма  
4. Кадр 3: s3 (bottom position) - норма
5. Кадр 4: ПЛОХАЯ ФОРМА (shallow + sagging)
6. Кадр 5: КРИТИЧЕСКИЕ SAFETY ОШИБКИ (overextension + piking)
7. Ожидает coaching results от Universal Processor

Убедитесь что:
- Kafka запущен на localhost:9092
- tisit-performance-svc запущен (python -m app.main)
- push-up-v2.json конфигурация обновлена
""")
        
        input("Нажмите Enter для начала теста...")
        
        # Отправка тестовых данных
        self.send_test_frames()
        
        print("\n⏳ Ожидание обработки (Universal Processor создает окна по 3 секунды)...")
        time.sleep(3)  # Даем время на обработку
        
        # Прослушивание результатов
        self.listen_for_results(timeout_seconds=30)
        
        print("\n✅ Тест завершен")


if __name__ == "__main__":
    print("🎯 Тестирование push-up упражнения")
    
    tester = PushupSystemTester()
    tester.run_full_test()