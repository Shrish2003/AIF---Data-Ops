import logging
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)


class RecommendationObjectBuilder:
    """
    -------------------------------------------------------
    Recommendation Object Builder

    Responsibility:
    Build the final Recommendation Object from a validated
    candidate recommendation (Gemini-generated or fallback).

    Input:
        Entity ID
        Validated Recommendation Dictionary (priority,
        recommendation, reason, expected_impact,
        estimated_recovery_time, confidence,
        automation_possible, human_approval_required,
        recommendation_source, selected_model — the last two
        set by the Multi-LLM Selection Agent)

    Output:
        Recommendation Object
    -------------------------------------------------------
    """

    # =====================================================
    # INITIALIZATION
    # =====================================================

    def __init__(self):

        logger.info("Recommendation Object Builder Ready.")

    # =====================================================
    # BUILD RECOMMENDATION OBJECT
    # =====================================================

    def build(self, entity_id, recommendation):

        recommendation_object = {

            "recommendation_id": str(uuid.uuid4()),

            "entity_id": entity_id,

            "priority": recommendation.get("priority"),

            "recommendation": recommendation.get("recommendation"),

            "reason": recommendation.get("reason"),

            "expected_impact": recommendation.get("expected_impact"),

            "estimated_recovery_time": recommendation.get(
                "estimated_recovery_time"
            ),

            # DEPRECATED: LLM-generated confidence is deprecated.
            "confidence": None if recommendation.get("recommendation_source") == "llm" else recommendation.get("confidence"),

            "automation_possible": recommendation.get(
                "automation_possible"
            ),

            "human_approval_required": recommendation.get(
                "human_approval_required"
            ),

            "recommendation_source": recommendation.get(
                "recommendation_source", "deterministic"
            ),

            "selected_model": recommendation.get("selected_model"),

            "generated_timestamp": datetime.now().isoformat()

        }

        return recommendation_object
