from typing import Dict, Any

from api.services.execution_data import load_execution_output

class RecommendationService:

    @staticmethod
    def _load_execution_output() -> Dict[str, Any]:
        return load_execution_output()

    @classmethod
    def get_recommendation(cls, pipeline_id: str) -> Dict[str, Any]:
        data = cls._load_execution_output()
        pid_str = str(pipeline_id)

        recommendation_obj = None
        for dataset, items in data.get("recommendation_output", {}).items():
            for item in items:
                if str(item.get("entity_id")) == pid_str:
                    recommendation_obj = item
                    break
            if recommendation_obj:
                break

        if not recommendation_obj:
            return {
                "pipeline_id": pipeline_id,
                "priority": None,
                "recommendation": None,
                "expected_impact": None,
                "recovery_time": None,
                "automation_possible": None,
                "human_approval_required": None,
                "confidence": None,
                "generated_by": None,
                "model": None,
                "recommendation_source": None
            }

        return {
            "pipeline_id": pipeline_id,
            "priority": recommendation_obj.get("priority"),
            "recommendation": recommendation_obj.get("recommendation") or recommendation_obj.get("enhanced_recommendation"),
            "expected_impact": recommendation_obj.get("expected_impact"),
            "recovery_time": recommendation_obj.get("estimated_recovery_time"),
            "automation_possible": recommendation_obj.get("automation_possible"),
            "human_approval_required": recommendation_obj.get("human_approval_required"),
            "confidence": recommendation_obj.get("confidence"),
            "generated_by": recommendation_obj.get("recommendation_source"),
            "model": recommendation_obj.get("selected_model"),
            "recommendation_source": recommendation_obj.get("recommendation_source")
        }
