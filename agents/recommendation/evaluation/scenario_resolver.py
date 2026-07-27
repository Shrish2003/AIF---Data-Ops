import logging

logger = logging.getLogger(__name__)

class ScenarioResolver:
    """
    ==========================================================
    Scenario Resolver
    
    Responsibility:
        Analyze operational context and identify the best
        matching Golden Truth scenario.
        
    Design:
        - Decoupled from storage (gets scenarios as a list).
        - Matches fields case-insensitively.
        - Scores scenarios by specificity and weights.
        - Selects highest score for reasonable match.
    ==========================================================
    """

    def __init__(self):
        logger.info("Scenario Resolver initialized.")

    def resolve(self, operational_context, scenarios):
        """
        Identify the best matching scenario based on the operational context using weighted scoring.
        
        Args:
            operational_context (dict): The context dictionary of the generated recommendation.
            scenarios (list): List of scenario dictionaries containing context keys and expected recommendations.
            
        Returns:
            dict or None: The resolved scenario dictionary or None if no match is found.
        """
        if not scenarios:
            logger.warning("No scenarios provided to Scenario Resolver.")
            return None

        import os
        # Check if we should limit resolution to representative entity to prevent Ragas rate-limit storms
        if os.environ.get("LIMIT_EVALUATION") == "true":
            rep_id = os.environ.get("REPRESENTATIVE_PIPELINE_ID")
            entity_id = operational_context.get("entity_id")
            if rep_id and entity_id and str(entity_id) != str(rep_id):
                return None

        # Define match weights for operational dimensions
        weights = {
            "platform": 10,
            "primary_failure_reason": 5,
            "execution_status": 5,
            "behavior_severity": 4,
            "risk_severity": 3,
            "integrity_status": 3
        }

        best_match = None
        best_score = -1.0

        for scenario in scenarios:
            scenario_context = scenario.get("context", {})
            if not scenario_context:
                continue

            score = 0.0
            match_count = 0

            # Enforce strict platform matching (different platforms are not compatible)
            scenario_platform = scenario_context.get("platform")
            actual_platform = operational_context.get("platform")
            if scenario_platform and actual_platform:
                if str(scenario_platform).strip().lower() != str(actual_platform).strip().lower():
                    continue

            for key, expected_val in scenario_context.items():
                expected_str = str(expected_val).strip().lower()
                actual_val = operational_context.get(key)

                if actual_val is None:
                    # Penalty for missing fields in context
                    score -= 1.0
                    continue

                actual_str = str(actual_val).strip().lower()
                # Normalize integrity status 'failed' to match 'fail' if necessary
                if key.strip().lower() == "integrity_status":
                    if actual_str == "failed":
                        actual_str = "fail"
                    if expected_str == "failed":
                        expected_str = "fail"

                weight = weights.get(key.strip().lower(), 2)

                if actual_str == expected_str:
                    score += weight
                    match_count += 1
                else:
                    score -= weight

            # Require positive score and at least one matching constraint for a valid match
            if score > 0.0 and match_count >= 1:
                if score > best_score:
                    best_score = score
                    best_match = scenario

        if best_match:
            logger.info(f"Resolved scenario: '{best_match.get('scenario_name')}' with score: {best_score}")
        else:
            logger.info("No matching Golden Truth scenario resolved.")

        return best_match
