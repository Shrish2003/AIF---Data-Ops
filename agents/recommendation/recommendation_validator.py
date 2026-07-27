import logging

logger = logging.getLogger(__name__)


class RecommendationValidator:
    """
    ==========================================================
    Recommendation Validator

    Responsibility:
        Validate that a candidate recommendation (from Gemini,
        or the deterministic fallback) has every required
        field, a valid priority, and a confidence value inside
        the configured range.

    Input:
        Candidate Recommendation Dictionary

    Output:
        (is_valid: bool, reasons: list[str])

    DOES NOT
    --------
    - Call Gemini
    - Decide what to do if validation fails (that is the
      Recommendation Agent's responsibility — retry once, then
      fall back)
    ==========================================================
    """

    # =====================================================
    # INITIALIZATION
    # =====================================================

    def __init__(self, rule_engine):

        self.rule_engine = rule_engine

        self.validation_config = self.rule_engine.get_validation_config()

        self.required_fields = self.validation_config.get(
            "required_fields", []
        )

        self.valid_priorities = self.validation_config.get(
            "valid_priorities", []
        )

        self.min_confidence = self.validation_config.get(
            "min_confidence", 0.0
        )

        self.max_confidence = self.validation_config.get(
            "max_confidence", 1.0
        )

        logger.info("Recommendation Validator Ready.")

    # =====================================================
    # VALIDATE
    # =====================================================

    def validate(self, candidate):

        reasons = []

        if not isinstance(candidate, dict):

            return False, ["Candidate recommendation is not a JSON object."]

        is_llm = candidate.get("recommendation_source") == "llm"
        for field in self.required_fields:
            if field == "confidence" and is_llm:
                continue
            if field not in candidate or candidate.get(field) in (None, ""):

                reasons.append(f"Missing required field: {field}")

        priority = candidate.get("priority")

        if (
            priority is not None
            and self.valid_priorities
            and priority not in self.valid_priorities
        ):

            reasons.append(
                f"Invalid priority: {priority!r} "
                f"(expected one of {self.valid_priorities})"
            )

        confidence = candidate.get("confidence")

        if confidence is not None:

            try:

                confidence_value = float(confidence)

                if not (
                    self.min_confidence
                    <= confidence_value
                    <= self.max_confidence
                ):

                    reasons.append(
                        f"Confidence {confidence_value} outside allowed "
                        f"range [{self.min_confidence}, "
                        f"{self.max_confidence}]"
                    )

            except (TypeError, ValueError):

                reasons.append(f"Confidence is not numeric: {confidence!r}")

        return (len(reasons) == 0), reasons
