from abc import ABC, abstractmethod


class LLMStrategy(ABC):
    @abstractmethod
    async def analyze(self, prompt: str, language: str) -> dict | None:
        """
        :param prompt: Text prompt for analysis
        :param language: Language to generate the response
        :return: Analysis result in JSON format
        """
        pass


class GeminiLLMStrategy(LLMStrategy):
    def __init__(self, api_key: str):
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')

    async def analyze(self, prompt: str, language: str = 'ru') -> dict | None:
        try:
            prompt_with_language = f"Provide the analysis in {language} language."
            self.model._system_instruction = prompt_with_language
            response = await self.model.generate_content_async(prompt)
            return response.text
        except Exception as e:
            print(f"Error in Gemini API call: {e}")
            return None


class LLMAnalyzer:
    def __init__(self, llm_strategy: LLMStrategy, exercise_name: str):
        self.llm_strategy = llm_strategy
        self.exercise_name = exercise_name

    async def analyze_exercise(self, exercise_summary: dict, language: str = 'ru') -> dict:
        prompt = self._generate_prompt(exercise_summary)
        return await self.llm_strategy.analyze(prompt, language)

    def _generate_prompt(self, exercise_summary: dict) -> str:
        return f"""
        As a professional trainer, analyze this {self.exercise_name} exercise feedback for safety and efficiency:

        Exercise Statistics:
        - Duration: {exercise_summary['exercise_duration']} seconds
        - Correct repetitions: {exercise_summary['exercise_performance']['correct']}
        - Incorrect repetitions: {exercise_summary['exercise_performance']['incorrect']}

        Timeline of issues:
        {self._format_timeline(exercise_summary['windows'])}

        Feedback summary:
        {self._format_feedback_summary(exercise_summary['feedback_summary'])}

        Please analyze:
        1. Are there any dangerous combinations of issues?
        2. What is the risk level for injury?
        3. What corrections are crucial for safety and improved form?
        4. Progress throughout the exercise
        5. How can the user improve their form in future sessions?
        6. Overall performance quality

        Provide your analysis in the specified language in JSON format with these fields:
        {{
            "safety_analysis": {{
                "status": "safe|warning|dangerous",
                "risk_level": 1-10,
                "critical_corrections": [],
            }},
            "performance_analysis": {{
                "quality_score": 1-10,
                "main_issues": [{{
                    "issue": str,
                    "severity": "low|medium|high",
                    "description": str
                }}],
                "progress_evaluation": str
            }},
            "recommendations": {{
                "critical_corrections": [],
                "improvement_suggestions": [],
                "future_session_tips": []
            }},
            "reasoning": "detailed explanation"
        }}
        """

    ## Alternative implementation
    # {{
    #     "safety_analysis": {{
    #         "status": "safe|warning|dangerous",
    #         "risk_level": 1 - 10,
    #         "critical_corrections": [],
    #     }},
    #     "performance_feedback": "",
    #     "improvement_suggestions": [],
    #     "reasoning": "detailed explanation"
    # }}

    @staticmethod
    def _format_timeline(windows: list[dict]) -> str:
        timeline = []
        for window in windows:
            window_feedback = [
                f"- {feedback_id} (persistence: {stats['frame_ratio']:.2f})"
                for feedback_id, stats in window['feedback_stats'].items()
            ]
            if window_feedback:
                timeline.append(
                    f"Time {window['start_time']:.1f}-{window['end_time']:.1f}s:\n" +
                    "\n".join(window_feedback)
                )
        return "\n\n".join(timeline)

    @staticmethod
    def _format_feedback_summary(feedback_summary: dict) -> str:
        return "\n".join([
            f"- {feedback_id}: occurred in {stats['window_ratio'] * 100:.1f}% of exercise time"
            for feedback_id, stats in feedback_summary.items()
        ])
