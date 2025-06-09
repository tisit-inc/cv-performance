import json
from datetime import datetime
from typing import Dict, Any, List
from quixstreams import Application
from quixstreams.models import TopicConfig

from app.core.config import get_settings
from app.cv.metrics_storage import MetricsStorage
from app.utils.logger import setup_logger


class QuixStreamProcessor:
    def __init__(self):
        self.settings = get_settings()
        self.logger = setup_logger("quix_stream_processor")
        self.metrics_storage = MetricsStorage(self.settings.metrics_dir)
        
        # Initialize Quix Streams Application
        self.app = Application(
            broker_address=self.settings.kafka_bootstrap_servers,
            consumer_group=self.settings.kafka_group_id,
            auto_offset_reset="earliest",
        )
        
        # Define topics
        self.input_topic = self.app.topic(
            self.settings.kafka_input_topic,
            config=TopicConfig(num_partitions=3, replication_factor=1)
        )
        self.output_topic = self.app.topic(
            "performance-results",
            config=TopicConfig(num_partitions=3, replication_factor=1)
        )

    def create_pipeline(self):
        """Create the beautiful Quix Streams processing pipeline"""
        self.logger.info("🚀 Creating Quix Streams pipeline...")
        
        # Create streaming dataframe from input topic
        sdf = self.app.dataframe(self.input_topic)
        
        # Parse JSON messages
        sdf = sdf.apply(self._parse_frame)
        
        # Filter out invalid frames
        sdf = sdf.filter(lambda frame: frame is not None)
        
        # Group by session for windowing
        sdf = sdf.group_by("session_uuid")
        
        # Create tumbling windows (2.5 seconds for feedback aggregation)
        sdf = sdf.tumbling_window(duration_ms=2500, grace_ms=500)
        
        # Process frames within each window
        sdf = sdf.apply(self._process_frame_in_window, stateful=True)
        
        # Aggregate feedback and exercise state within windows
        sdf = sdf.reduce(self._aggregate_window_data, self._create_window_state)
        
        # Emit window results when window closes
        sdf = sdf.final()
        
        # Process session completion
        sdf = sdf.apply(self._check_session_completion, stateful=True)
        
        # Filter out incomplete sessions
        sdf = sdf.filter(lambda result: result.get("session_complete", False))
        
        # Send results to output topic
        sdf = sdf.to_topic(self.output_topic)
        
        return sdf

    def _parse_frame(self, message: bytes) -> Dict[str, Any] | None:
        """Parse incoming Kafka message"""
        try:
            data = json.loads(message.decode("utf-8"))
            self.logger.debug(f"📥 Parsed frame: {data['frame_track_uuid']}")
            return data
        except Exception as e:
            self.logger.error(f"❌ Failed to parse frame: {e}")
            return None

    def _process_frame_in_window(self, frame: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        """Process individual frame within a window with state"""
        try:
            # Get exercise metrics
            exercise_name = frame.get('exercise')
            if exercise_name not in state.get('exercise_metrics', {}):
                exercise_metrics = self.metrics_storage.get_metrics_for_exercise(exercise_name)
                if 'exercise_metrics' not in state:
                    state['exercise_metrics'] = {}
                state['exercise_metrics'][exercise_name] = exercise_metrics
            else:
                exercise_metrics = state['exercise_metrics'][exercise_name]
            
            # Extract angles
            angles = {
                angle['name']: angle['value']
                for angle in frame['metrics']['angles']
            }
            
            # Determine current exercise state
            current_state = self._get_exercise_state(angles, exercise_metrics)
            
            # Check thresholds and generate feedback
            feedback_ids = self._check_thresholds(angles, current_state, exercise_metrics)
            
            # Add processed data to frame
            frame['processed'] = {
                'exercise_state': current_state,
                'feedback_ids': feedback_ids,
                'angles': angles,
                'processed_at': datetime.now().timestamp()
            }
            
            self.logger.debug(f"✅ Processed frame {frame['frame_track_uuid']} - state: {current_state}")
            return frame
            
        except Exception as e:
            self.logger.error(f"❌ Error processing frame: {e}")
            frame['processed'] = {'error': str(e)}
            return frame

    def _get_exercise_state(self, angles: Dict[str, float], exercise_metrics: Dict) -> str:
        """Determine exercise state from angles using state machine"""
        states = exercise_metrics['state_machine']['beginner']['states']
        
        for state in states:
            try:
                # Safe evaluation using restricted environment
                safe_vars = {k: v for k, v in angles.items() if isinstance(v, (int, float))}
                if eval(state["condition"], {"__builtins__": {}}, safe_vars):
                    return state["id"]
            except Exception:
                continue
        
        return "s1"  # Default state

    def _check_thresholds(self, angles: Dict[str, float], current_state: str, exercise_metrics: Dict) -> List[str]:
        """Check angle thresholds and generate feedback IDs"""
        feedback_ids = []
        
        if current_state == 's1':
            return feedback_ids
            
        thresholds = exercise_metrics['thresholds']['beginner']
        
        for angle_name, angle_thresholds in thresholds.get("angle_thresh", {}).items():
            if angle_name not in angles:
                continue
                
            angle_value = angles[angle_name]
            for threshold in angle_thresholds:
                min_val = threshold.get("min")
                max_val = threshold.get("max")
                
                if ((min_val is not None and angle_value < min_val) or 
                    (max_val is not None and angle_value > max_val)):
                    feedback_id = exercise_metrics['feedback_messages'][threshold['feedback']]['id']
                    feedback_ids.append(feedback_id)
        
        return feedback_ids

    def _create_window_state(self) -> Dict[str, Any]:
        """Create initial state for window aggregation"""
        return {
            'frames_processed': 0,
            'feedback_stats': {},
            'exercise_states': [],
            'state_sequence': [],
            'curls': {'correct': 0, 'incorrect': 0},
            'flags': {'incorrect_posture': False, 'critical_error': False},
            'window_start': None,
            'window_end': None,
            'session_uuid': None,
            'exercise': None
        }

    def _aggregate_window_data(self, window_state: Dict[str, Any], frame: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate frame data within window"""
        processed = frame.get('processed', {})
        
        # Update window metadata
        if window_state['session_uuid'] is None:
            window_state['session_uuid'] = frame['session_uuid']
            window_state['exercise'] = frame['exercise']
            window_state['window_start'] = frame['timestamp']
        
        window_state['window_end'] = frame['timestamp']
        window_state['frames_processed'] += 1
        
        # Aggregate feedback
        feedback_ids = processed.get('feedback_ids', [])
        for feedback_id in feedback_ids:
            if feedback_id not in window_state['feedback_stats']:
                window_state['feedback_stats'][feedback_id] = {'count': 0}
            window_state['feedback_stats'][feedback_id]['count'] += 1
        
        # Track exercise states
        exercise_state = processed.get('exercise_state')
        if exercise_state:
            window_state['exercise_states'].append(exercise_state)
            
            # Update state sequence for rep counting
            if not window_state['state_sequence'] or exercise_state != window_state['state_sequence'][-1]:
                window_state['state_sequence'].append(exercise_state)
        
        self.logger.debug(f"📊 Window aggregate - frames: {window_state['frames_processed']}, state: {exercise_state}")
        return window_state

    def _check_session_completion(self, window_result: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        """Check if session is complete and emit final results"""
        session_uuid = window_result['session_uuid']
        
        # Track session state across windows
        if session_uuid not in state:
            state[session_uuid] = {
                'total_curls': {'correct': 0, 'incorrect': 0},
                'all_feedback': {},
                'last_activity': datetime.now().timestamp(),
                'windows_processed': 0
            }
        
        session_state = state[session_uuid]
        session_state['windows_processed'] += 1
        session_state['last_activity'] = datetime.now().timestamp()
        
        # Aggregate feedback across windows
        for feedback_id, stats in window_result.get('feedback_stats', {}).items():
            if feedback_id not in session_state['all_feedback']:
                session_state['all_feedback'][feedback_id] = {'total_count': 0, 'windows_seen': 0}
            session_state['all_feedback'][feedback_id]['total_count'] += stats['count']
            session_state['all_feedback'][feedback_id]['windows_seen'] += 1
        
        # Check for session completion (simplified logic)
        current_time = datetime.now().timestamp()
        is_session_timeout = (current_time - session_state['last_activity']) > 30  # 30 seconds timeout
        
        # Check if this is the last frame of video
        is_video_complete = False
        if 'video_duration' in window_result and 'current_time' in window_result:
            is_video_complete = window_result['current_time'] >= window_result['video_duration'] - 0.1
        
        if is_session_timeout or is_video_complete:
            # Generate final session summary
            final_result = {
                'session_uuid': session_uuid,
                'session_complete': True,
                'exercise': window_result.get('exercise'),
                'completion_reason': 'video_end' if is_video_complete else 'timeout',
                'total_windows': session_state['windows_processed'],
                'exercise_performance': session_state['total_curls'],
                'feedback_summary': self._generate_feedback_summary(session_state['all_feedback']),
                'completed_at': datetime.now().isoformat()
            }
            
            # Clean up session state
            del state[session_uuid]
            
            self.logger.info(f"🎯 Session {session_uuid} completed - {final_result['completion_reason']}")
            return final_result
        
        # Return intermediate result (will be filtered out)
        return {
            'session_uuid': session_uuid,
            'session_complete': False,
            'window_summary': window_result
        }

    def _generate_feedback_summary(self, all_feedback: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary of feedback across all windows"""
        summary = {}
        
        for feedback_id, stats in all_feedback.items():
            summary[feedback_id] = {
                'total_occurrences': stats['total_count'],
                'windows_seen': stats['windows_seen'],
                'frequency_ratio': stats['windows_seen']  # Can be normalized later
            }
        
        return summary

    def run(self):
        """Start the beautiful stream processing pipeline"""
        self.logger.info("🌟 Starting Quix Streams Performance Processor...")
        
        # Create and run pipeline
        sdf = self.create_pipeline()
        
        try:
            self.app.run(sdf)
        except KeyboardInterrupt:
            self.logger.info("🛑 Stream processor stopped by user")
        except Exception as e:
            self.logger.error(f"💥 Stream processor error: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    processor = QuixStreamProcessor()
    processor.run()