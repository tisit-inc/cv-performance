#!/usr/bin/env python3
"""
🧪 Тест Universal Quality Assessment System

Отправляет тестовые CV данные в Kafka и проверяет работу системы
"""

import json
import asyncio
import time
from datetime import datetime
from confluent_kafka import Producer, Consumer
from typing import Dict, Any

# Тестовые CV данные (симуляция реального потока)
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

SAMPLE_DUMBBELL_FRAMES = [
    {
        "frame_track_uuid": "test_frame_001",
        "timestamp": datetime.now().isoformat(),
        "exercise": "dumbbell-arm",
        "session_uuid": "test_session_001",
        "video_duration": 30.0,
        "current_time": 1.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "elbow_angle", "value": 120.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},
                {"name": "shoulder_angle", "value": 15.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_frame_002", 
        "timestamp": datetime.now().isoformat(),
        "exercise": "dumbbell-arm",
        "session_uuid": "test_session_001",
def __init__(self):
    self.bootstrap_servers = "localhost:9092"
    self.cv_topic = "inference_results"

    # Kafka setup
    self.consumer = Consumer({
        'bootstrap.servers': self.bootstrap_servers,
        'group.id': f'cv-debug-{int(time.time())}',
        'auto.offset.reset': 'latest'
    })

    self.consumer.subscribe([self.cv_topic])
    print(f"🔍 Debugging CV data from: {self.cv_topic}")
    print("=" * 80)

def debug_angles(self, data):
    """Анализ углов от CV сервиса"""
    metrics = data.get('metrics', {})
    angles = metrics.get('angles', [])

    print(f"\nFrame: {data.get('frame_track_uuid', 'unknown')}")
    print(f"Session: {data.get('session_uuid', 'unknown')[:8]}")
    print(f"Exercise: {data.get('exercise', 'unknown')}")

    print("\nAngles from CV:")
    for angle in angles:
        name = angle.get('name', 'unknown')
        value = angle.get('value', 0)
        print(f"  {name}: {value:.1f}°")

    # Предсказание FSM состояния
    elbow_angle = None
    for angle in angles:
        if angle.get('name') == 'elbow_angle':
            elbow_angle = angle.get('value', 0)
            break

    if elbow_angle is not None:
        if elbow_angle > 110:
            predicted_state = "s1 (rest)"
        elif elbow_angle > 70:
            predicted_state = "s2 (lifting)"
        else:
            predicted_state = "s3 (lowering)"

        print(f"  → Predicted FSM state: {predicted_state}")

        # Проверка проблем
        if elbow_angle < 30:
            print(f"  ⚠️ VERY LOW elbow angle - possible angle calculation issue!")
        elif elbow_angle > 170:
            print(f"  ⚠️ VERY HIGH elbow angle - possible angle calculation issue!")

    # Проверка offset_angle для safety
    offset_angle = None
    for angle in angles:
        if angle.get('name') == 'offset_angle':
            offset_angle = angle.get('value', 0)
            break

    if offset_angle is not None:
        if abs(offset_angle) > 15:
            print(f"  🚨 SAFETY: offset_angle {offset_angle:.1f}° > 15° threshold")
        else:
            print(f"  ✅ SAFETY: offset_angle {offset_angle:.1f}° is OK")

    print("-" * 60)

def run(self):
    """Основной цикл отладки"""
    print("🚀 Starting CV data debugger... (Ctrl+C to stop)")

    try:
        while True:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"❌ Error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                self.debug_angles(data)

            except Exception as e:
                print(f"❌ Parse error: {e}")

    except KeyboardInterrupt:
        print("\n🛑 Debugger stopped")
    finally:
        self.consumer.close()
        "video_duration": 30.0,
        "current_time": 2.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "elbow_angle", "value": 85.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # s2 состояние
                {"name": "shoulder_angle", "value": 20.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    },
    {
        "frame_track_uuid": "test_frame_003",
        "timestamp": datetime.now().isoformat(), 
        "exercise": "dumbbell-arm",
        "session_uuid": "test_session_001",
        "video_duration": 30.0,
        "current_time": 3.0,
        "landmarks": [{"x": 0.5, "y": 0.7, "z": -0.3} for _ in range(33)],
        "metrics": {
            "angles": [
                {"name": "elbow_angle", "value": 55.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.4}, "ref_point": {"x": 0.6, "y": 1.1}},  # s3 состояние
                {"name": "shoulder_angle", "value": 45.0, "point1": {"x": 0.5, "y": 0.7}, "point2": {"x": 0.6, "y": 1.1}, "ref_point": {"x": 0.6, "y": 1.1}}  # Критическая ошибка техники!
            ],
            "dominant_side": {"side": "left", "probability": 0.8}
        }
    }
]

class UniversalSystemTester:
    def __init__(self):
        # Kafka конфигурация (должна совпадать с вашей)
        self.producer_config = {
            'bootstrap.servers': 'localhost:9092',  # Настройте под ваш Kafka
            'client.id': 'test_producer'
        }
        
        self.consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test_consumer_group_' + str(int(time.time())),  # Уникальная группа
            'auto.offset.reset': 'latest',
            'client.id': 'test_consumer'
        }
        
        self.input_topic = "performance-input"  # Откуда читает Universal Processor
        self.output_topic = "performance-feedback"  # Куда пишет результаты
        
        try:
            self.producer = Producer(self.producer_config)
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe([self.output_topic])
            print("✅ Kafka connection established")
        except Exception as e:
            print(f"❌ Kafka connection failed: {e}")
            print("💡 Убедитесь что Kafka запущен на localhost:9092")
            raise

    def send_test_frames(self, exercise_type="dumbbell-arm"):
        """Отправка тестовых кадров в Kafka"""
        print(f"🚀 Отправка тестовых CV данных для {exercise_type}...")
        
        if exercise_type == "squat":
            test_frames = SAMPLE_SQUAT_FRAMES
        else:
            test_frames = SAMPLE_DUMBBELL_FRAMES
        
        for i, frame in enumerate(test_frames):
            # Обновляем timestamp для реалистичности
            frame["timestamp"] = datetime.now().isoformat()
            
            message = json.dumps(frame)
            
            self.producer.produce(
                topic=self.input_topic,
                key=frame["session_uuid"],
                value=message
            )
            
            main_angle = frame['metrics']['angles'][0]
            angle_name = main_angle['name']
            angle_value = main_angle['value']
            
            if exercise_type == "squat":
                if angle_value > 160:
                    state = "s1 (standing)"
                elif angle_value > 90:
                    state = "s2 (descending)"
                else:
                    state = "s3 (deep squat)"
            else:
                state = f"s{2 if angle_value > 70 else 3}"
            
            print(f"📤 Отправлен кадр {i+1}: {angle_name}={angle_value}, state={state}")
            
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
        """Красивый вывод результата coaching"""
        print("\n" + "="*60)
        print("🎯 COACHING RESULT RECEIVED")
        print("="*60)
        
        print(f"📋 Session: {result.get('session_id', 'unknown')}")
        print(f"🏋️  Exercise: {result.get('exercise_type', 'unknown')}")
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

    def run_full_test(self, exercise_type="dumbbell-arm"):
        """Полный тест системы"""
        test_descriptions = {
            "dumbbell-arm": {
                "frames": "3 кадра с гантелями",
                "states": "s1 (rest) → s2 (lifting) → s3 (lowering) + ошибки"
            },
            "squat": {
                "frames": "4 кадра с приседаниями", 
                "states": "s1 (standing) → s2 (descending) → s3 (deep) → КРИТИЧЕСКИЕ SAFETY ОШИБКИ"
            }
        }
        
        desc = test_descriptions.get(exercise_type, test_descriptions["dumbbell-arm"])
        
        print(f"""
🧪 UNIVERSAL QUALITY ASSESSMENT SYSTEM TEST - {exercise_type.upper()}
============================================

Этот тест:
1. Отправляет {desc['frames']}
2. Переходы: {desc['states']}
3. Ожидает coaching results от Universal Processor

Убедитесь что:
- Kafka запущен на localhost:9092
- tisit-performance-svc запущен (python -m app.main)
- Топики созданы (performance-input, performance-feedback)
""")
        
        input("Нажмите Enter для начала теста...")
        
        # Отправка тестовых данных
        self.send_test_frames(exercise_type)
        
        print("\n⏳ Ожидание обработки (Universal Processor создает окна по 3 секунды)...")
        time.sleep(2)  # Даем время на обработку
        
        # Прослушивание результатов
        self.listen_for_results(timeout_seconds=30)
        
        print("\n✅ Тест завершен")


if __name__ == "__main__":
    import sys
    
    # Выбор упражнения из аргументов командной строки
    exercise_type = "dumbbell-arm"  # По умолчанию
    if len(sys.argv) > 1:
        exercise_type = sys.argv[1]
    
    print(f"🎯 Тестирование упражнения: {exercise_type}")
    
    tester = UniversalSystemTester()
    tester.run_full_test(exercise_type)