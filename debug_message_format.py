#!/usr/bin/env python3
"""
Debug script to capture the exact message format sent by performance service
and compare with expected CV demo format
"""

import json
from datetime import datetime
from app.services.performance_service import PerformanceService

def test_message_format():
    """Test the _prepare_llm_semantic_input method to see exact output format"""
    
    # Create a mock coaching result that would normally come from the pipeline
    mock_coaching_result = {
        'session_uuid': 'test-session-123',
        'exercise': 'squat',
        'timestamp': datetime.now().isoformat(),
        'semantic_analysis': {
            'window_duration_ms': 3000,
            'frames_analyzed': 10,
            'fsm_transitions': ['s1→s2', 's2→s3'],
            'semantic_patterns': {
                'FORM_KNEE_VALGUS_S2': {
                    'count': 3,
                    'first_seen': '2025-06-10T12:00:00',
                    'category': 'form_deviation',
                    'semantic_category': 'form_deviation',
                    'coaching_priority': 'high',
                    'description': 'Knee tracking inward during squat descent',
                    'contexts': [
                        {'exercise_state': 's2', 'metrics': {'angle_value': 45.0}},
                        {'exercise_state': 's2', 'metrics': {'angle_value': 42.0}}
                    ]
                }
            },
            'coaching_categories': {
                'safety_concerns': [],
                'technical_issues': ['FORM_KNEE_VALGUS_S2'],
                'form_deviations': [],
                'execution_problems': []
            }
        },
        'requires_coaching': True,
        'coaching_metadata': {
            'windows_processed': 5,
            'coaching_count': 2
        }
    }
    
    # Create service instance
    service = PerformanceService()
    
    # Call the method we want to test
    llm_input = service._prepare_llm_semantic_input(mock_coaching_result)
    
    print("=== PERFORMANCE SERVICE OUTPUT FORMAT ===")
    print(json.dumps(llm_input, indent=2, default=str))
    
    print("\n=== KEY FIELD ANALYSIS ===")
    print(f"Session identifier: '{llm_input.get('session_id', 'NOT_FOUND')}'")
    print(f"Exercise type: '{llm_input.get('exercise_type', 'NOT_FOUND')}'")
    print(f"Timestamp field: '{llm_input.get('analysis_timestamp', 'NOT_FOUND')}'")
    
    print("\n=== EXPECTED CV DEMO FORMAT (from monitor) ===")
    expected_cv_format = {
        "session_id": "test-session-123",
        "exercise_type": "squat", 
        "analysis_timestamp": "2025-06-10T12:00:00",
        "semantic_context": {
            "analysis_window_ms": 3000,
            "movement_flow": ["s1→s2", "s2→s3"],
            "error_patterns_detected": 1
        },
        "coaching_focus_areas": {
            "safety_priorities": [],
            "technique_improvements": [
                {
                    "error_code": "FORM_KNEE_VALGUS_S2",
                    "description": "Knee tracking inward during squat descent",
                    "frequency": 3,
                    "coaching_priority": "high",
                    "context_examples": [
                        {"exercise_state": "s2", "metrics": {"angle_value": 45.0}},
                        {"exercise_state": "s2", "metrics": {"angle_value": 42.0}}
                    ]
                }
            ],
            "form_corrections": [],
            "execution_guidance": []
        },
        "llm_processing_hints": {
            "urgency_level": "high",
            "coaching_complexity": 1,
            "session_maturity": 5,
            "intervention_history": 2
        }
    }
    
    print(json.dumps(expected_cv_format, indent=2))
    
    print("\n=== FIELD COMPARISON ===")
    
    # Compare key fields
    fields_to_check = [
        ('session_id', 'session_id'),
        ('exercise_type', 'exercise_type'), 
        ('analysis_timestamp', 'analysis_timestamp'),
        ('semantic_context', 'semantic_context'),
        ('coaching_focus_areas', 'coaching_focus_areas'),
        ('llm_processing_hints', 'llm_processing_hints')
    ]
    
    for field_name, expected_field in fields_to_check:
        actual_value = llm_input.get(field_name, 'MISSING')
        expected_value = expected_cv_format.get(expected_field, 'MISSING')
        
        if actual_value == 'MISSING':
            print(f"❌ MISSING: {field_name}")
        elif isinstance(actual_value, dict) and isinstance(expected_value, dict):
            print(f"✅ PRESENT: {field_name} (dict with {len(actual_value)} keys)")
        else:
            print(f"✅ PRESENT: {field_name} = {actual_value}")

if __name__ == "__main__":
    test_message_format()