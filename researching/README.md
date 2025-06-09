# Research & Development Scripts

This directory contains experimental, testing, and development scripts for the tisit-performance-svc project.

## Structure

### `/tests/v2_current/` - Current architecture tests
- `test_1_squat_quality_assessment.py` - Tests for squat exercise quality assessment
- `test_2_pushup_quality_assessment.py` - Tests for push-up exercise quality assessment

### `/tests/v1_legacy/` - Legacy architecture tests  
- Contains old test files from previous Redis-based architecture

### `/debug/` - Debug utilities
- `debug_cv_data.py` - Debug script for monitoring CV service data

## Usage

All scripts are standalone and can be run independently for testing and development purposes.

**Note:** These files are NOT part of the production service and should not be deployed.

## Testing the Current Architecture

1. Start the performance service:
   ```bash
   cd /Users/egorken/PycharmProjects/tisit-performance-svc
   python -m app.main
   ```

2. Run exercise-specific tests:
   ```bash
   # Test squat quality assessment
   python researching/tests/v2_current/test_1_squat_quality_assessment.py
   
   # Test push-up quality assessment  
   python researching/tests/v2_current/test_2_pushup_quality_assessment.py
   ```

## Current Architecture (v3.0)

- **Universal Quality Processor** with Quix Streams
- **Semantic Error Taxonomy v2.0**
- **Multi-exercise support** (dumbbell-arm, squat, push-up)
- **Range safety detection**
- **FSM-based state tracking**