from typing import Dict

class CopilotService:

    @classmethod
    def chat(cls, question: str) -> dict:
        q = question.lower()
        if "bro0001" in q:
            reply = (
                "Entity BRO0001 failed because of an execution failure and a major metric anomaly. "
                "The throughput reached 607 msg/sec (+21.4% above baseline) with consumer lag rising to 57. "
                "The Integrity Agent reported record count validation warnings, and a HIGH risk of pipeline failure was detected. "
                "Recommended action is to inspect the validation errors, check upstream schemas, and potentially scale consumers."
            )
            suggested_actions = [
                "Inspect the validation errors for BRO0001",
                "Check upstream schemas",
                "Scale Kubernetes consumer pods"
            ]
            confidence = "high"
            follow_up_questions = [
                "Should we scale the consumer instance?",
                "Do you want to see the detailed lineage graph?"
            ]
        else:
            reply = (
                f"For query '{question}', the Copilot resolved that the AIF pipeline status is stable, and all agents report UP status. "
                "You can specify a pipeline ID such as BRO0001 for specific failure root causes."
            )
            suggested_actions = [
                "Run execution pipeline",
                "View dashboard metrics"
            ]
            confidence = "medium"
            follow_up_questions = [
                "Is there a specific pipeline you are investigating?",
                "Do you want to run a new execute command?"
            ]

        return {
            "reply": reply,
            "suggestedActions": suggested_actions,
            "confidence": confidence,
            "followUpQuestions": follow_up_questions
        }
