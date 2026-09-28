"""
==========================================================
Recommendation Evidence Service  — Phase 4.5
==========================================================

Responsibility:
    Aggregate historical recommendation evidence from:
    1. recommendation_context  — lookup data (recommendation,
       impact, applied, success_rate fields from
       recommendation_history.csv via ContextEnricher)
    2. incident_context        — lookup incident history
    3. historical_context      — Phase 3 ChromaDB matches
       (action_taken, outcome, resolution_status per match)

    This service performs ZERO ChromaDB queries, ZERO Ollama
    calls, and ZERO CSV reads.  All three inputs are already
    available in-memory from the behavior_object.

EPISTEMIC RULE:
    Historical success is evidence, NOT a guarantee.
    Output must say: "Historically successful in 4 of 5 similar
    incidents."
    NOT: "This recommendation will solve the issue."

Output:
    {
        "available": true,
        "recommendation": "...",
        "applied_previously": true,
        "success_rate": 95,
        "historical_usage_count": 5,
        "successful_count": 4,
        "evidence_summary": "...",
        "evidence": [...]
    }
==========================================================
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_UNAVAILABLE = {
    "available": False,
    "recommendation": "Not available",
    "applied_previously": None,
    "success_rate": None,
    "historical_usage_count": 0,
    "successful_count": 0,
    "evidence_summary": "No recommendation evidence available.",
    "evidence": []
}

_RESOLVED_STATUSES = frozenset({
    "resolved", "closed", "fixed", "recovered",
    "success", "succeeded", "complete", "completed"
})


def _is_valid(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip().lower()
    return s not in ("", "none", "null", "not available", "not determined",
                     "unknown", "n/a", "nan")


def _safe_str(value: Any, default: str = "Not available") -> str:
    return str(value).strip() if _is_valid(value) else default


class RecommendationEvidenceService:
    """
    Aggregates recommendation evidence from lookup context and
    historical ChromaDB matches already retrieved by Phase 3.
    Presents historical success rates without claiming guarantees.
    """

    # ----------------------------------------------------------
    # Public interface
    # ----------------------------------------------------------

    @staticmethod
    def analyze(
        recommendation_context: Dict[str, Any],
        incident_context: Dict[str, Any],
        historical_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Produce structured recommendation evidence.

        Parameters
        ----------
        recommendation_context : dict
            From entity.context.recommendation_context.
            Fields: recommendation, impact, applied, success_rate.
        incident_context : dict
            From entity.context.incident_context.
            Fields: total_incidents, root_cause, resolution, severity.
        historical_context : dict
            From behavior_object["historical_context"] (Phase 3 output).
            Matches carry: action_taken, outcome, resolution_status,
            recommendation.

        Returns
        -------
        dict
        """
        try:
            return RecommendationEvidenceService._analyze(
                recommendation_context, incident_context, historical_context
            )
        except Exception as exc:
            logger.warning(
                "RecommendationEvidenceService.analyze failed gracefully: %s",
                exc
            )
            return dict(_UNAVAILABLE)

    # ----------------------------------------------------------
    # Internal implementation
    # ----------------------------------------------------------

    @staticmethod
    def _analyze(
        recommendation_context: Dict[str, Any],
        incident_context: Dict[str, Any],
        historical_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        has_rec_ctx = bool(recommendation_context)
        has_hist = (
            isinstance(historical_context, dict)
            and historical_context.get("available")
            and historical_context.get("matches")
        )

        if not has_rec_ctx and not has_hist:
            return dict(_UNAVAILABLE)

        # ── Lookup recommendation context fields ───────────────
        rec_text = _safe_str(recommendation_context.get("recommendation"))
        rec_impact = _safe_str(recommendation_context.get("impact"))
        applied_raw = recommendation_context.get("applied")
        applied_previously: Optional[bool] = None
        if _is_valid(applied_raw):
            applied_previously = str(applied_raw).strip().lower() in (
                "yes", "true", "1"
            )

        success_rate_raw = recommendation_context.get("success_rate")
        lookup_success_rate: Optional[int] = None
        try:
            if success_rate_raw is not None:
                lookup_success_rate = int(float(str(success_rate_raw)))
        except (ValueError, TypeError):
            pass

        # ── Aggregate historical match evidence ────────────────
        evidence_items: List[Dict[str, Any]] = []
        historical_successful = 0
        historical_total = 0

        if has_hist:
            matches = historical_context.get("matches", [])
            for match in matches:
                action = match.get("action_taken")
                outcome = match.get("outcome")
                resolution = match.get("resolution_status")
                h_rec = match.get("recommendation")
                memory_id = match.get("memory_id", "")
                incident_id = match.get("incident_id", "")

                # Determine outcome label
                outcome_label = None
                if _is_valid(outcome):
                    outcome_label = str(outcome).strip()
                elif _is_valid(resolution):
                    outcome_label = str(resolution).strip()

                is_success = (
                    outcome_label is not None
                    and outcome_label.lower() in _RESOLVED_STATUSES
                )

                if _is_valid(action) or _is_valid(h_rec):
                    historical_total += 1
                    if is_success:
                        historical_successful += 1

                    evidence_items.append({
                        "memory_id": memory_id,
                        "incident_id": incident_id,
                        "action_taken": _safe_str(action),
                        "recommendation": _safe_str(h_rec),
                        "outcome": _safe_str(outcome_label),
                        "was_successful": is_success
                    })

        # ── Build evidence summary ─────────────────────────────
        summary_parts = []

        if historical_total > 0:
            summary_parts.append(
                f"Historically successful in {historical_successful} of "
                f"{historical_total} similar incident"
                f"{'s' if historical_total != 1 else ''} "
                f"recorded in operational memory."
            )

        if lookup_success_rate is not None:
            summary_parts.append(
                f"Lookup recommendation history reports a success rate of "
                f"{lookup_success_rate}%."
            )

        if applied_previously is True:
            summary_parts.append(
                "This recommendation has been applied to this entity previously."
            )
        elif applied_previously is False:
            summary_parts.append(
                "This recommendation has not been applied to this entity previously."
            )

        if rec_impact and rec_impact != "Not available":
            summary_parts.append(f"Expected impact from lookup: {rec_impact}.")

        summary_parts.append(
            "Note: Historical success rates are evidence, not guarantees."
        )

        evidence_summary = (
            " ".join(summary_parts)
            if summary_parts
            else "No recommendation evidence available."
        )

        # Need at least one useful data point
        has_useful_data = (
            rec_text != "Not available"
            or historical_total > 0
            or lookup_success_rate is not None
        )
        if not has_useful_data:
            return dict(_UNAVAILABLE)

        return {
            "available": True,
            "recommendation": rec_text,
            "applied_previously": applied_previously,
            "success_rate": lookup_success_rate,
            "historical_usage_count": historical_total,
            "successful_count": historical_successful,
            "evidence_summary": evidence_summary,
            "evidence": evidence_items
        }
