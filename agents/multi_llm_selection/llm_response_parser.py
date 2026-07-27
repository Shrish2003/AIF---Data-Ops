import logging

logger = logging.getLogger(__name__)


class LLMResponseValidationError(Exception):
    """
    Raised when a parsed LLM response is missing a required
    field, has an invalid priority/confidence, or otherwise
    cannot be trusted as-is. The Multi-LLM Selection Agent
    catches this and asks LLMConfig's selection order for the
    next available provider; it never propagates to the
    Recommendation Agent.
    """
    pass


class LLMResponseParser:
    """
    ==========================================================
    LLM Response Parser

    Responsibility:
        Validate a parsed LLM response dictionary (already
        produced by GeminiClient / GrokClient / OllamaClient —
        each of those guarantees a Python dict, never a raw
        string) against the Recommendation Object's required
        fields, reject malformed or hallucinated-looking
        responses, and return a clean, normalized dictionary.

    Input:
        Parsed LLM Response Dictionary

    Output:
        Normalized Recommendation Dictionary (raises
        LLMResponseValidationError if the response cannot be
        trusted)

    DOES NOT
    --------
    - Call any LLM
    - Decide which provider to try next (that is the Multi-LLM
      Selection Agent's responsibility)
    - Calculate priority, category, or any deterministic value
      (those come from the Recommendation Engine and are only
      cross-checked here, never recalculated)
    ==========================================================
    """

    REQUIRED_FIELDS = [
        "priority",
        "recommendation",
        "reason",
        "expected_impact",
        "estimated_recovery_time",
        "automation_possible",
        "human_approval_required",
        "generated_by",
    ]

    VALID_PRIORITIES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

    VALID_CATEGORIES = [
        "GL Account Reconciliation",
        "Tax Calculation Run",
        "billing_pipeline",
        "asset_management_etl",
        "asset.maintenance.alerts",
        "marketing.campaign.events",
    ]

    BOOLEAN_FIELDS = ["automation_possible", "human_approval_required"]

    # =====================================================
    # INITIALIZATION
    # =====================================================

    def __init__(self, min_confidence=0.0, max_confidence=1.0):

        self.min_confidence = min_confidence

        self.max_confidence = max_confidence

        logger.info("LLM Response Parser Ready.")

    # =====================================================
    # COERCE BOOLEAN
    #
    # LLMs sometimes return "true"/"false" strings instead of
    # real JSON booleans — accepted defensively, never silently
    # dropped.
    # =====================================================

    def _coerce_boolean(self, value):

        if isinstance(value, bool):

            return value

        if isinstance(value, str):

            lowered = value.strip().lower()

            if lowered in ("true", "yes", "1"):

                return True

            if lowered in ("false", "no", "0"):

                return False

        return None

    # =====================================================
    # VALIDATE AGAINST EXPECTED (ALREADY-DECIDED) VALUES
    #
    # The prompt tells the LLM the recommendation has already
    # been determined and must not be changed. If the LLM
    # returns a priority or category that conflicts with what
    # the deterministic engine already produced, that is
    # treated as a hallucinated/conflicting response and
    # rejected outright rather than silently overridden.
    # =====================================================

    def _check_conflicts(self, candidate, expected_priority,
                          expected_category):

        conflicts = []

        candidate_priority = candidate.get("priority")

        if expected_priority and candidate_priority and \
                candidate_priority != expected_priority:

            conflicts.append(
                f"LLM returned priority {candidate_priority!r} but the "
                f"deterministic engine already decided "
                f"{expected_priority!r}."
            )

        candidate_category = candidate.get("category")

        if expected_category and candidate_category and \
                candidate_category != expected_category:

            conflicts.append(
                f"LLM returned category {candidate_category!r} but the "
                f"deterministic engine already decided "
                f"{expected_category!r}."
            )

        return conflicts

    # =====================================================
    # PARSE / VALIDATE
    # =====================================================

    def parse(self, raw_response, expected_priority=None,
              expected_category=None):

        if not isinstance(raw_response, dict):

            raise LLMResponseValidationError(
                "LLM response is not a JSON object."
            )

        # Pre-validation JSON repair and normalization
        normalized = dict(raw_response)

        # Ensure recommendation and enhanced_recommendation are mapped to each other
        if "enhanced_recommendation" not in normalized or normalized.get("enhanced_recommendation") in (None, ""):
            if "recommendation" in normalized and normalized.get("recommendation") not in (None, ""):
                normalized["enhanced_recommendation"] = normalized["recommendation"]

        if "recommendation" not in normalized or normalized.get("recommendation") in (None, ""):
            if "enhanced_recommendation" in normalized and normalized.get("enhanced_recommendation") not in (None, ""):
                normalized["recommendation"] = normalized["enhanced_recommendation"]

        reasons = []

        for field in self.REQUIRED_FIELDS:

            if field not in normalized or \
                    normalized.get(field) in (None, ""):

                reasons.append(f"Missing required field: {field}")

        if reasons:

            raise LLMResponseValidationError(
                f"LLM response failed validation: {reasons}"
            )

        priority = normalized.get("priority")

        if priority not in self.VALID_PRIORITIES:
            if expected_priority in self.VALID_PRIORITIES:
                normalized["priority"] = expected_priority
                priority = expected_priority
            else:
                raise LLMResponseValidationError(
                    f"Invalid priority: {priority!r} "
                    f"(expected one of {self.VALID_PRIORITIES})"
                )

        category = normalized.get("category")

        if category is not None and category not in self.VALID_CATEGORIES:

            logger.warning(
                f"LLM returned an unrecognized category: {category!r}. "
                "Keeping it, but it does not match the known category "
                "list."
            )

        for field in self.BOOLEAN_FIELDS:

            coerced = self._coerce_boolean(normalized.get(field))

            if coerced is None:

                raise LLMResponseValidationError(
                    f"Field {field!r} is not a valid boolean: "
                    f"{normalized.get(field)!r}"
                )

            normalized[field] = coerced

        conflicts = self._check_conflicts(
            normalized, expected_priority, expected_category
        )

        if conflicts:

            raise LLMResponseValidationError(
                f"LLM response conflicts with the deterministic "
                f"recommendation: {conflicts}"
            )

        # Force LLM recommendation confidence to None
        normalized["confidence"] = None

        return normalized
