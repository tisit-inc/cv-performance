#!/usr/bin/env python3
"""
🎯 Monitoring Coaching Results
Читает результаты от Universal Quality Processor и показывает их в удобном формате
"""

import json
import time
from datetime import datetime
from confluent_kafka import Consumer
from typing import Dict, Any

class CoachingResultsMonitor:
    def __init__(self):
        self.bootstrap_servers = "localhost:9092"
        self.output_topic = "performance-feedback"
        
        # Kafka setup
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f'coaching-monitor-{int(time.time())}',
            'auto.offset.reset': 'latest'  # Only new messages
        })
        
        self.consumer.subscribe([self.output_topic])
        print(f"🎯 Monitoring coaching results from: {self.output_topic}")
        print("=" * 80)

    def format_coaching_result(self, result: Dict[str, Any]):
        """Красивый вывод результата coaching"""
        print("\n" + "🎯 " + "="*70)
        print(f"📋 Session: {result.get('session_id', 'unknown')}")
        print(f"🏋️  Exercise: {result.get('exercise_type', 'unknown')}")
        print(f"⏰ Time: {result.get('analysis_timestamp', 'unknown')}")
        
        # Семантический контекст
        semantic_context = result.get('semantic_context', {})
        print(f"\n📊 Analysis Window: {semantic_context.get('analysis_window_ms', 0)}ms")
        print(f"🔄 Movement Flow: {' → '.join(semantic_context.get('movement_flow', []))}")
        print(f"⚠️  Detected {semantic_context.get('error_patterns_detected', 0)} error patterns")
        
        # Coaching focus areas
        focus_areas = result.get('coaching_focus_areas', {})
        
        # Safety issues (critical)
        safety = focus_areas.get('safety_priorities', [])
        if safety:
            print(f"\n🚨 SAFETY PRIORITIES ({len(safety)}):")
            for issue in safety:
                print(f"   🔴 {issue.get('error_code', 'unknown')}")
                print(f"      {issue.get('description', 'no description')}")
                print(f"      Frequency: {issue.get('frequency', 0)}, Priority: {issue.get('coaching_priority', 'unknown')}")
        
        # Technical improvements
        technical = focus_areas.get('technique_improvements', [])
        if technical:
            print(f"\n🎯 TECHNIQUE IMPROVEMENTS ({len(technical)}):")
            for issue in technical:
                print(f"   🟡 {issue.get('error_code', 'unknown')}")
                print(f"      {issue.get('description', 'no description')}")
                print(f"      Frequency: {issue.get('frequency', 0)}, Priority: {issue.get('coaching_priority', 'unknown')}")
        
        # Form corrections
        form = focus_areas.get('form_corrections', [])
        if form:
            print(f"\n💪 FORM CORRECTIONS ({len(form)}):")
            for issue in form:
                print(f"   🟠 {issue.get('error_code', 'unknown')}")
                print(f"      {issue.get('description', 'no description')}")
                print(f"      Frequency: {issue.get('frequency', 0)}, Priority: {issue.get('coaching_priority', 'unknown')}")
        
        # Execution guidance
        execution = focus_areas.get('execution_guidance', [])
        if execution:
            print(f"\n⚡ EXECUTION GUIDANCE ({len(execution)}):")
            for issue in execution:
                print(f"   🔵 {issue.get('error_code', 'unknown')}")
                print(f"      {issue.get('description', 'no description')}")
                print(f"      Frequency: {issue.get('frequency', 0)}, Priority: {issue.get('coaching_priority', 'unknown')}")
        
        # LLM processing hints
        hints = result.get('llm_processing_hints', {})
        print(f"\n🤖 LLM PROCESSING HINTS:")
        print(f"   Urgency: {hints.get('urgency_level', 'unknown')}")
        print(f"   Complexity: {hints.get('coaching_complexity', 0)} patterns")
        print(f"   Session Maturity: {hints.get('session_maturity', 0)} windows")
        print(f"   Interventions: {hints.get('intervention_history', 0)} times")
        
        # Summary
        total_issues = len(safety) + len(technical) + len(form) + len(execution)
        if total_issues == 0:
            print(f"\n✅ NO ISSUES DETECTED - Great form!")
        else:
            print(f"\n📈 TOTAL ISSUES: {total_issues}")
            if safety:
                print("   ⚠️  IMMEDIATE ATTENTION NEEDED (Safety)")
            elif technical:
                print("   📝 Technical improvements recommended")
            elif form:
                print("   💪 Form adjustments suggested")
            
        print("=" * 80)

    def run(self):
        """Основной цикл мониторинга"""
        print("🚀 Starting coaching results monitor...")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    print(f"❌ Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Parse coaching result
                    result = json.loads(msg.value().decode('utf-8'))
                    self.format_coaching_result(result)
                    
                except Exception as e:
                    print(f"❌ Error parsing result: {e}")
                    print(f"Raw message: {msg.value()}")
                    
        except KeyboardInterrupt:
            print("\n🛑 Monitor stopped by user")
        finally:
            self.consumer.close()
            print("✅ Monitor shutdown complete")


if __name__ == "__main__":
    monitor = CoachingResultsMonitor()
    monitor.run()