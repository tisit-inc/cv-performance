#!/usr/bin/env python3
"""
🎯 CV Feedback Integration
Простой способ показать результаты coaching в CV сервисе
"""

import json
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional

# Простая заглушка без Kafka для демонстрации
class MockCoachingFeedback:
    def __init__(self):
        self.latest_feedback = None
        self.feedback_history = []
        
    def update_feedback(self, session_id: str, exercise: str, feedback_data: Dict[str, Any]):
        """Обновление feedback данных"""
        self.latest_feedback = {
            'session_id': session_id,
            'exercise': exercise,
            'timestamp': datetime.now().strftime("%H:%M:%S"),
            'feedback': feedback_data
        }
        self.feedback_history.append(self.latest_feedback)
        
        # Ограничиваем историю
        if len(self.feedback_history) > 10:
            self.feedback_history = self.feedback_history[-10:]
    
    def get_display_text(self) -> str:
        """Получить текст для отображения на экране CV"""
        if not self.latest_feedback:
            return "✅ Waiting for feedback..."
        
        fb = self.latest_feedback
        lines = [
            f"🎯 Coaching Feedback ({fb['timestamp']})",
            f"Session: {fb['session_id'][:8]}",
            ""
        ]
        
        feedback_data = fb.get('feedback', {})
        
        # Извлекаем основные проблемы
        safety_issues = feedback_data.get('safety_priorities', [])
        technique_issues = feedback_data.get('technique_improvements', [])
        form_issues = feedback_data.get('form_corrections', [])
        
        if safety_issues:
            lines.append("🚨 SAFETY ISSUES:")
            for issue in safety_issues[:2]:  # Показываем только первые 2
                lines.append(f"  • {issue.get('error_code', 'unknown')}")
        
        if technique_issues:
            lines.append("🎯 TECHNIQUE:")
            for issue in technique_issues[:2]:
                lines.append(f"  • {issue.get('error_code', 'unknown')}")
                
        if form_issues:
            lines.append("💪 FORM:")
            for issue in form_issues[:2]:
                lines.append(f"  • {issue.get('error_code', 'unknown')}")
        
        if not (safety_issues or technique_issues or form_issues):
            lines.append("✅ Great form! No issues detected")
        
        return "\\n".join(lines)

# Глобальный объект для интеграции
coaching_feedback = MockCoachingFeedback()

def simulate_coaching_data():
    """Симулируем данные coaching для демонстрации"""
    
    # Пример данных с проблемами
    sample_feedback_1 = {
        'safety_priorities': [
            {'error_code': 'SAFETY_POSTURE_S1', 'description': 'Body alignment issue'},
        ],
        'technique_improvements': [
            {'error_code': 'TECHNIQUE_ROM_MAJOR_S2', 'description': 'Range of motion too small'},
        ]
    }
    
    # Пример данных без проблем
    sample_feedback_2 = {
        'safety_priorities': [],
        'technique_improvements': [],
        'form_corrections': []
    }
    
    coaching_feedback.update_feedback("test-session", "dumbbell-arm", sample_feedback_1)
    time.sleep(5)
    coaching_feedback.update_feedback("test-session", "dumbbell-arm", sample_feedback_2)

def draw_coaching_overlay(frame, coaching_feedback_obj):
    """
    Функция для добавления в CV сервис
    Рисует overlay с coaching feedback на кадре
    """
    import cv2
    
    feedback_text = coaching_feedback_obj.get_display_text()
    
    # Параметры текста
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 0.5
    color = (255, 255, 255)  # Белый
    thickness = 1
    
    # Позиция overlay (правый верхний угол)
    x_start = frame.shape[1] - 300
    y_start = 30
    
    # Фон для текста
    overlay = frame.copy()
    cv2.rectangle(overlay, (x_start - 10, y_start - 20), 
                  (frame.shape[1] - 10, y_start + 200), (0, 0, 0), -1)
    cv2.addWeighted(overlay, 0.7, frame, 0.3, 0, frame)
    
    # Рисуем текст построчно
    lines = feedback_text.split('\\n')
    for i, line in enumerate(lines):
        y_pos = y_start + (i * 20)
        cv2.putText(frame, line, (x_start, y_pos), font, font_scale, color, thickness)
    
    return frame

if __name__ == "__main__":
    # Демонстрация
    print("🎯 CV Feedback Integration Demo")
    print("="*50)
    
    # Симулируем данные
    threading.Thread(target=simulate_coaching_data, daemon=True).start()
    
    # Показываем как это выглядит
    for i in range(10):
        print(f"\\nFrame {i+1}:")
        print(coaching_feedback.get_display_text())
        time.sleep(2)