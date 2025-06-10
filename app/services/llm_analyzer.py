"""
LLM Analyzer Service for Fitness Coaching
Uses Google Gemini API to generate human-readable coaching advice
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional

from google import genai
from quixstreams.models import TopicConfig

from app.core.config import get_settings
from app.core.managers import QuixStreamsManager
from app.utils.logger import setup_logger


class LLMAnalyzer:
    """
    LLM-powered fitness coaching analyzer
    Converts semantic performance data into human-readable coaching advice
    """

    def __init__(self):
        self.settings = get_settings()
        self.logger = setup_logger(__name__)

        # Initialize Gemini client
        if not self.settings.GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY must be set in environment")

        self.gemini_client = genai.Client(api_key=self.settings.GEMINI_API_KEY)

        # Quix Streams Manager
        self.stream_manager = QuixStreamsManager(self.logger)

        # Topics
        self.input_topic = self.stream_manager.get_topic(
            self.settings.KAFKA_LLM_INPUT_TOPIC, config=TopicConfig(num_partitions=1, replication_factor=1)
        )
        self.output_topic = self.stream_manager.get_topic(
            self.settings.KAFKA_LLM_OUTPUT_TOPIC, config=TopicConfig(num_partitions=1, replication_factor=1)
        )

    def create_llm_pipeline(self):
        """Create LLM processing pipeline"""
        self.logger.info("Creating LLM coaching pipeline")

        sdf = self.stream_manager.app.dataframe(self.input_topic)

        # Parse LLM input messages
        sdf = sdf.apply(self._parse_llm_input)
        sdf = sdf.filter(lambda msg: msg is not None)

        # Generate coaching advice with Gemini
        sdf = sdf.apply(self._generate_coaching_advice)

        # Send to output topic
        sdf = sdf.to_topic(self.output_topic)

        return sdf

    def _parse_llm_input(self, message) -> Optional[Dict[str, Any]]:
        """Parse incoming LLM input message"""
        try:
            if isinstance(message, bytes):
                data = json.loads(message.decode("utf-8"))
            elif isinstance(message, dict):
                data = message
            else:
                data = json.loads(str(message))

            self.logger.debug("LLM input - Session: %s", data.get('session_id', 'unknown'))
            return data

        except Exception as e:
            self.logger.error("LLM input parse error: %s", e)
            return None

    def _generate_coaching_advice(self, llm_input: Dict[str, Any]) -> Dict[str, Any]:
        """Generate coaching advice using Gemini API"""
        try:
            session_id = llm_input.get('session_id', 'unknown')
            exercise_type = llm_input.get('exercise_type', 'unknown')

            # Create prompt from semantic data
            prompt = self._create_coaching_prompt(llm_input)

            # Generate response with Gemini
            response = self.gemini_client.models.generate_content(
                model=self.settings.GEMINI_MODEL,
                contents=[{"role": "user", "parts": [{"text": prompt}]}],
                config={
                    "temperature": self.settings.LLM_TEMPERATURE,
                    "max_output_tokens": self.settings.LLM_MAX_OUTPUT_TOKENS,
                    "top_p": self.settings.LLM_TOP_P,
                    "top_k": self.settings.LLM_TOP_K,
                },
            )

            # Format output message
            coaching_output = {
                "session_id": session_id,
                "exercise_type": exercise_type,
                "timestamp": datetime.now().isoformat(),
                "coaching_advice": response.text,
                "source": "llm_gemini",
                "model": self.settings.GEMINI_MODEL,
                "urgency_level": llm_input.get('llm_processing_hints', {}).get('urgency_level', 'low'),
                "original_semantic_data": llm_input.get('semantic_analysis', {}),
            }

            self.logger.info(
                "Generated coaching advice - Session: %s, Exercise: %s, Urgency: %s",
                session_id,
                exercise_type,
                coaching_output['urgency_level'],
            )

            return coaching_output

        except Exception as e:
            self.logger.error("Error generating coaching advice: %s", e)
            return {
                "session_id": llm_input.get('session_id', 'unknown'),
                "exercise_type": llm_input.get('exercise_type', 'unknown'),
                "timestamp": datetime.now().isoformat(),
                "coaching_advice": "Unable to generate coaching advice at this time. Please continue with your exercise.",
                "source": "llm_error",
                "error": str(e),
                "urgency_level": "low",
            }

    def _create_coaching_prompt(self, llm_input: Dict[str, Any]) -> str:
        """Create coaching prompt from semantic data"""
        exercise_type = llm_input.get('exercise_type', 'unknown')
        semantic_analysis = llm_input.get('semantic_analysis', {})
        focus_areas = llm_input.get('coaching_focus_areas', {})
        hints = llm_input.get('llm_processing_hints', {})

        # Get exercise-specific system prompt
        system_prompt = self._get_exercise_system_prompt(exercise_type)

        # Extract key information
        safety_issues = focus_areas.get('safety_priorities', [])
        technique_issues = focus_areas.get('technique_improvements', [])
        form_issues = focus_areas.get('form_corrections', [])
        execution_issues = focus_areas.get('execution_guidance', [])

        urgency = hints.get('urgency_level', 'low')
        movement_flow = semantic_analysis.get('movement_flow', [])

        # Build the prompt
        prompt = f"""{system_prompt}

EXERCISE: {exercise_type.upper()}
URGENCY LEVEL: {urgency.upper()}
MOVEMENT FLOW: {' → '.join(movement_flow[-3:]) if movement_flow else 'No data'}

ANALYSIS RESULTS:"""

        if safety_issues:
            prompt += f"\n\nSAFETY CONCERNS ({len(safety_issues)}):"
            for issue in safety_issues[:2]:  # Top 2 safety issues
                code = issue.get('error_code', 'Unknown')
                description = issue.get('description', 'No description')
                frequency = issue.get('frequency', 0)
                prompt += f"\n- {code}: {description} (occurred {frequency} times)"

        if technique_issues:
            prompt += f"\n\nTECHNIQUE ISSUES ({len(technique_issues)}):"
            for issue in technique_issues[:2]:  # Top 2 technique issues
                code = issue.get('error_code', 'Unknown')
                description = issue.get('description', 'No description')
                frequency = issue.get('frequency', 0)
                prompt += f"\n- {code}: {description} (occurred {frequency} times)"

        if form_issues:
            prompt += f"\n\nFORM CORRECTIONS ({len(form_issues)}):"
            for issue in form_issues[:2]:  # Top 2 form issues
                code = issue.get('error_code', 'Unknown')
                description = issue.get('description', 'No description')
                frequency = issue.get('frequency', 0)
                prompt += f"\n- {code}: {description} (occurred {frequency} times)"

        if execution_issues:
            prompt += f"\n\nEXECUTION GUIDANCE ({len(execution_issues)}):"
            for issue in execution_issues[:1]:  # Top 1 execution issue
                code = issue.get('error_code', 'Unknown')
                description = issue.get('description', 'No description')
                frequency = issue.get('frequency', 0)
                prompt += f"\n- {code}: {description} (occurred {frequency} times)"

        if not (safety_issues or technique_issues or form_issues or execution_issues):
            prompt += "\n\nNo issues detected - excellent form!"

        prompt += "\n\nPlease provide coaching advice based on the above analysis. Focus on the most critical issues first, especially safety concerns."

        return prompt

    def _get_exercise_system_prompt(self, exercise_type: str) -> str:
        """Get exercise-specific system prompt"""

        base_prompt = """You are an expert fitness coach and personal trainer. Your role is to provide clear, actionable, and encouraging coaching advice based on movement analysis data.

Guidelines:
- Prioritize safety issues above all else
- Be encouraging but direct about corrections needed
- Use simple, clear language
- Focus on 1-2 main points maximum
- Provide specific actionable steps
- Keep responses concise (2-3 sentences)
- Use positive reinforcement when possible"""

        exercise_prompts = {
            "squat": """
SQUAT COACHING EXPERTISE:
- Focus on knee alignment (knees should track over toes)
- Emphasize proper depth (thighs parallel to ground)
- Monitor back posture (neutral spine)
- Watch for weight distribution (heels grounded)
- Check for proper hip hinge movement""",
            "push-up": """
PUSH-UP COACHING EXPERTISE:
- Focus on body alignment (straight line from head to heels)
- Monitor elbow position (45-degree angle from body)
- Check core engagement
- Watch for proper hand placement
- Ensure full range of motion""",
            "dumbbell-arm": """
DUMBBELL ARM COACHING EXPERTISE:
- Focus on controlled movement (no swinging)
- Monitor elbow stability
- Check for proper shoulder engagement
- Watch for core stability
- Ensure proper range of motion""",
        }

        exercise_specific = exercise_prompts.get(exercise_type, "Focus on proper form and safety.")

        return f"{base_prompt}\n\n{exercise_specific}"

    def run(self):
        """Run LLM Analyzer Service"""
        self.logger.info("Starting LLM Analyzer Service")

        try:
            # Create pipeline and start processing
            self.create_llm_pipeline()
            self.stream_manager.run()

        except KeyboardInterrupt:
            self.logger.info("LLM Analyzer Service stopped by user")
        except Exception as e:
            self.logger.exception("LLM Analyzer Service error: %s", e)
            raise
        finally:
            self.logger.info("LLM Analyzer Service shutdown complete")


if __name__ == "__main__":
    analyzer = LLMAnalyzer()
    analyzer.run()
