#!/usr/bin/env python3
"""
🎯 Compact Coaching Monitor
Показывает только суммарную информацию без дублей
"""

import json
import time
from datetime import datetime
from confluent_kafka import Consumer
from typing import Dict, Any
from collections import Counter

class CompactCoachingMonitor:
    def __init__(self):
        self.bootstrap_servers = "localhost:9092"
        self.output_topic = "performance-feedback"
        
        # Kafka setup
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f'compact-monitor-{int(time.time())}',
            'auto.offset.reset': 'latest'
        })
        
        self.consumer.subscribe([self.output_topic])
        print(f"🎯 Compact Coaching Monitor - Topic: {self.output_topic}")
        print("=" * 60)

    def format_compact_result(self, result: Dict[str, Any]):
        """Компактный вывод результата"""
        session_id = result.get('session_id', 'unknown')[:8]
        exercise = result.get('exercise_type', 'unknown')
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        print(f"\n⏰ {timestamp} | 📋 {session_id} | 🏋️ {exercise}")
        
        # Подсчет уникальных ошибок
        focus_areas = result.get('coaching_focus_areas', {})
        
        # Счетчики для каждой категории
        safety_codes = set()
        technical_codes = set()
        form_codes = set()
        execution_codes = set()
        
        for issue in focus_areas.get('safety_priorities', []):
            safety_codes.add(issue.get('error_code', 'unknown'))
            
        for issue in focus_areas.get('technique_improvements', []):
            technical_codes.add(issue.get('error_code', 'unknown'))
            
        for issue in focus_areas.get('form_corrections', []):
            form_codes.add(issue.get('error_code', 'unknown'))
            
        for issue in focus_areas.get('execution_guidance', []):
            execution_codes.add(issue.get('error_code', 'unknown'))
        
        # Компактный вывод
        if safety_codes:
            print(f"🚨 SAFETY ({len(safety_codes)}): {', '.join(list(safety_codes)[:3])}")
        if technical_codes:
            print(f"🎯 TECHNIQUE ({len(technical_codes)}): {', '.join(list(technical_codes)[:3])}")
        if form_codes:
            print(f"💪 FORM ({len(form_codes)}): {', '.join(list(form_codes)[:3])}")
        if execution_codes:
            print(f"⚡ EXECUTION ({len(execution_codes)}): {', '.join(list(execution_codes)[:3])}")
        
        total_issues = len(safety_codes) + len(technical_codes) + len(form_codes) + len(execution_codes)
        
        if total_issues == 0:
            print("✅ NO ISSUES - Great form!")
        else:
            urgency = result.get('llm_processing_hints', {}).get('urgency_level', 'low')
            print(f"📊 Total: {total_issues} unique issues | Urgency: {urgency}")
        
        print("-" * 60)

    def run(self):
        """Основной цикл мониторинга"""
        print("🚀 Starting compact monitor... (Ctrl+C to stop)")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    print(f"❌ Error: {msg.error()}")
                    continue
                
                try:
                    result = json.loads(msg.value().decode('utf-8'))
                    self.format_compact_result(result)
                    
                except Exception as e:
                    print(f"❌ Parse error: {e}")
                    
        except KeyboardInterrupt:
            print("\n🛑 Monitor stopped")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    monitor = CompactCoachingMonitor()
    monitor.run()