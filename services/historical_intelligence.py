"""
==========================================================
Historical Intelligence Service  — Phase 4.1
==========================================================

Responsibility:
    Aggregate and analyze already-retrieved historical context
    (historical_context["matches"] from Phase 3 ChromaDB retrieval)
    into structured patterns, recurring root causes, outcome counts,
    and a normalized evidence summary.

    This service performs ZERO additional ChromaDB queries,
    ZERO Ollama calls, and ZERO CSV reads.
    It operates exclusively on the in-memory historical_context
    dict produced by OperationalMemoryRetriever.

Input:
    historical_context dict (from behavior_object["historical_context"])

Output:
    historical_intelligence dict

IMPORTANT EPISTEMIC RULE:
    All findings must be framed as historical evidence.
    Never present historical data as current ground truth.
    Use language such as "Historical evidence suggests..."
    not "Root cause is..." or "Current state is...".
==========================================================
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_UNAVAILABLE = {
    "available": False,
    "match_count": 0,
    "incident_type_frequency": {},
    "recurring_root_causes": [],
    "recurring_symptoms": [],
    "historical_recommendations": [],
    "historical_outcomes": {},
    "successful_actions": [],
    "failed_actions": [],
    "summary": "No historical evidence available."
}


def _is_valid(value: Any) -> bool:
    """Return True if value represents a meaningful, non-empty string."""
    if value is None:
        return False
    s = str(value).strip().lower()
    return s not in ("", "none", "null", "not available", "not determined",
                     "unknown", "n/a", "nan")


class HistoricalIntelligenceService:
    """
    Aggregates Phase 3 ChromaDB matches into structured historical
    intelligence: incident frequency, root cause recurrence, outcome
    counts, and a concise evidence summary.

    All results are clearly labelled as historical evidence.
    """

    # ----------------------------------------------------------
    # Public interface
    # ----------------------------------------------------------

    @staticmethod
    def analyze(historical_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Produce a structured historical intelligence summary from
        the already-retrieved historical_context dict.

        Parameters
        ----------
        historical_context : dict
            Output of OperationalMemoryRetriever.retrieve_historical_evidence().
            Expected keys: available, retrieval_count, matches.

        Returns
        -------
        dict
            Structured historical intelligence, always with an
            "available" bool field.
        """
        try:
            return HistoricalIntelligenceService._analyze(historical_context)
        except Exception as exc:
            logger.warning(
                "HistoricalIntelligenceService.analyze failed gracefully: %s", exc
            )
            return dict(_UNAVAILABLE)

    # ----------------------------------------------------------
    # Internal implementation
    # ----------------------------------------------------------

    @staticmethod
    def _analyze(historical_context: Dict[str, Any]) -> Dict[str, Any]:
        if not historical_context or not historical_context.get("available"):
            return dict(_UNAVAILABLE)

        matches: List[Dict[str, Any]] = historical_context.get("matches") or []
        if not matches:
            return dict(_UNAVAILABLE)

        # ---- Counters ----------------------------------------
        incident_type_freq: Dict[str, int] = {}
        root_cause_count: Dict[str, int] = {}
        symptom_count: Dict[str, int] = {}
        recommendation_list: List[str] = []
        outcome_count: Dict[str, int] = {}
        successful_actions: List[str] = []
        failed_actions: List[str] = []

        for match in matches:
            # Incident type
            inc_type = match.get("incident_type")
            if _is_valid(inc_type):
                incident_type_freq[inc_type] = incident_type_freq.get(inc_type, 0) + 1

            # Root cause
            root_cause = match.get("root_cause")
            if _is_valid(root_cause):
                root_cause_count[root_cause] = root_cause_count.get(root_cause, 0) + 1

            # Symptoms — stored as a single string in ChromaDB metadata
            # Split on common separators to enumerate individual symptoms.
            symptoms_raw = match.get("metadata", {}).get("symptoms") \
                if isinstance(match.get("metadata"), dict) else None
            # Also check flat match keys (normalised by retriever)
            if not symptoms_raw:
                symptoms_raw = match.get("symptoms")
            if _is_valid(symptoms_raw):
                for sym in str(symptoms_raw).split(","):
                    sym = sym.strip()
                    if sym:
                        symptom_count[sym] = symptom_count.get(sym, 0) + 1

            # Recommendation
            rec = match.get("recommendation")
            if _is_valid(rec) and rec not in recommendation_list:
                recommendation_list.append(rec)

            # Outcome
            outcome = match.get("outcome")
            resolution = match.get("resolution_status")
            outcome_label = None
            if _is_valid(outcome):
                outcome_label = str(outcome).strip()
            elif _is_valid(resolution):
                outcome_label = str(resolution).strip()

            if outcome_label:
                outcome_count[outcome_label] = outcome_count.get(outcome_label, 0) + 1

            # Successful / failed actions
            action = match.get("action_taken")
            if _is_valid(action):
                resolved_statuses = {"resolved", "closed", "fixed", "recovered",
                                     "success", "succeeded"}
                if outcome_label and outcome_label.lower() in resolved_statuses:
                    if action not in successful_actions:
                        successful_actions.append(action)
                else:
                    if action not in failed_actions:
                        failed_actions.append(action)

        # ---- Ranked lists ------------------------------------
        recurring_root_causes = [
            {"cause": cause, "count": count}
            for cause, count in sorted(
                root_cause_count.items(), key=lambda x: x[1], reverse=True
            )
        ]

        recurring_symptoms = [
            {"symptom": sym, "count": count}
            for sym, count in sorted(
                symptom_count.items(), key=lambda x: x[1], reverse=True
            )
        ]

        # ---- Summary sentence --------------------------------
        match_count = len(matches)
        summary_parts = [
            f"Historical evidence suggests {match_count} similar "
            f"incident{'s' if match_count != 1 else ''} found in operational memory."
        ]

        if recurring_root_causes:
            top = recurring_root_causes[0]
            summary_parts.append(
                f"The most common historical root cause was "
                f"'{top['cause']}' ({top['count']} occurrence"
                f"{'s' if top['count'] != 1 else ''})."
            )

        if outcome_count:
            resolved_total = sum(
                v for k, v in outcome_count.items()
                if k.lower() in {"resolved", "closed", "fixed", "recovered",
                                  "success", "succeeded"}
            )
            if resolved_total:
                summary_parts.append(
                    f"Historical resolution rate: "
                    f"{resolved_total} of {match_count} incidents resolved."
                )

        summary = " ".join(summary_parts)

        return {
            "available": True,
            "match_count": match_count,
            "incident_type_frequency": incident_type_freq,
            "recurring_root_causes": recurring_root_causes,
            "recurring_symptoms": recurring_symptoms,
            "historical_recommendations": recommendation_list,
            "historical_outcomes": outcome_count,
            "successful_actions": successful_actions,
            "failed_actions": failed_actions,
            "summary": summary
        }
