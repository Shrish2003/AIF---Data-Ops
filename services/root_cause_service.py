"""
==========================================================
Root Cause Service  — Phase 4.2
==========================================================

Responsibility:
    Produce evidence-weighted, deterministic root cause analysis
    (RCA) hypotheses from current telemetry, historical intelligence,
    and lookup incident context.

    CRITICAL RULES:
    ─────────────────────────────────────────────────────
    1. Never claim a root cause solely from historical memory.
    2. Every hypothesis must carry explicit evidence items.
    3. Confidence is computed by a documented, deterministic
       scoring rule — never by an LLM, never arbitrarily.
    4. Each hypothesis must declare its source:
         "current"    → current telemetry alone
         "historical" → historical data alone
         "combined"   → both current + historical
    5. Identical inputs must always produce identical outputs.
    ─────────────────────────────────────────────────────

APPROVED CONFIDENCE SCORING RULE:
    Current CRITICAL deviation        +0.30
    Current WARNING deviation         +0.15
    Current operational event         +0.10
    Integrity corroboration            +0.10   (future-proof, unused now)
    Historical root cause, 1 match     +0.10
    Historical root cause, 2+ matches  +0.20
    Historical root cause, 3+ matches  +0.30
    incident_context root-cause match  +0.15
    Cap at 1.0.

    Evidence points are per-hypothesis and never double-counted.
    An identical fact cannot award points twice.

Confidence levels:
    >= 0.70 → HIGH
    0.40-0.69 → MEDIUM
    < 0.40  → LOW

This service performs ZERO ChromaDB queries, ZERO Ollama calls,
and ZERO CSV reads.
==========================================================
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_UNAVAILABLE = {
    "available": False,
    "primary_hypotheses": [],
    "explanation": "Insufficient evidence to form root cause hypotheses."
}

# ──────────────────────────────────────────────────────────────
# Confidence thresholds
# ──────────────────────────────────────────────────────────────
_CONFIDENCE_HIGH = 0.70
_CONFIDENCE_MEDIUM = 0.40


def _confidence_level(score: float) -> str:
    if score >= _CONFIDENCE_HIGH:
        return "HIGH"
    if score >= _CONFIDENCE_MEDIUM:
        return "MEDIUM"
    return "LOW"


def _is_valid(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip().lower()
    return s not in ("", "none", "null", "not available", "not determined",
                     "unknown", "n/a", "nan")


class RootCauseService:
    """
    Evidence-weighted, deterministic root cause analysis.

    Generates hypotheses from current telemetry (behavior deviations,
    observer events, anomaly patterns) and historical support
    (recurring root causes from historical_intelligence + lookup
    incident_context).  Current and historical evidence are always
    clearly labelled.
    """

    # ----------------------------------------------------------
    # Public interface
    # ----------------------------------------------------------

    @staticmethod
    def analyze(
        behavior_object: Dict[str, Any],
        historical_intelligence: Dict[str, Any],
        incident_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Produce a structured RCA result.

        Parameters
        ----------
        behavior_object : dict
            Full behavior object including observation and behavior analysis.
        historical_intelligence : dict
            Output of HistoricalIntelligenceService.analyze().
        incident_context : dict
            Lookup incident_context from entity.context.incident_context.

        Returns
        -------
        dict
            available, primary_hypotheses, explanation
        """
        try:
            return RootCauseService._analyze(
                behavior_object, historical_intelligence, incident_context
            )
        except Exception as exc:
            logger.warning(
                "RootCauseService.analyze failed gracefully: %s", exc
            )
            return dict(_UNAVAILABLE)

    # ----------------------------------------------------------
    # Internal implementation
    # ----------------------------------------------------------

    @staticmethod
    def _analyze(
        behavior_object: Dict[str, Any],
        historical_intelligence: Dict[str, Any],
        incident_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        # ── Extract current evidence ──────────────────────────
        behavior = behavior_object.get("behavior", {})
        deviation = behavior.get("deviation", {})
        patterns = behavior.get("patterns", [])
        severity = behavior.get("severity", "NORMAL")

        obs = behavior_object.get("observation", {})
        events = obs.get("observations", {}).get("events", [])
        drift = behavior.get("drift", {})

        # ── Current severity signal ────────────────────────────
        has_critical = severity == "CRITICAL"
        has_warning = severity == "WARNING"

        # Max deviation across all metrics
        max_dev_pct = 0.0
        max_dev_metric = None
        worst_dev_severity = "NORMAL"
        for metric, dev_info in deviation.items():
            dev_pct = abs(dev_info.get("deviation_percent", 0.0) or 0.0)
            dev_sev = dev_info.get("severity", "NORMAL")
            if dev_pct > max_dev_pct:
                max_dev_pct = dev_pct
                max_dev_metric = metric
                worst_dev_severity = dev_sev

        # ── Historical root cause list ─────────────────────────
        hist_available = (
            isinstance(historical_intelligence, dict)
            and historical_intelligence.get("available")
        )
        recurring_causes = []
        if hist_available:
            recurring_causes = historical_intelligence.get(
                "recurring_root_causes", []
            )

        # ── incident_context root cause ────────────────────────
        ic_root_cause = None
        if _is_valid(incident_context.get("root_cause")):
            ic_root_cause = str(incident_context["root_cause"]).strip()

        # ── Build hypothesis list ──────────────────────────────
        hypotheses: List[Dict[str, Any]] = []

        # ── STEP 1: Current-evidence hypothesis ────────────────
        # Generate a hypothesis from current telemetry when significant
        # anomalies exist.
        if has_critical or has_warning or events or patterns:
            cause_parts = []
            if patterns:
                cause_parts.append(f"{', '.join(patterns)}")
            if max_dev_metric and worst_dev_severity in ("CRITICAL", "WARNING"):
                cause_parts.append(
                    f"{max_dev_metric} metric deviation ({max_dev_pct:.1f}%)"
                )
            if events:
                cause_parts.append(f"{', '.join(events)}")

            cause_label = (
                "; ".join(cause_parts)
                if cause_parts
                else "Anomalous operational behavior detected"
            )

            evidence = []
            conf = 0.0

            if worst_dev_severity == "CRITICAL":
                conf += 0.30
                evidence.append(
                    f"Current CRITICAL deviation on {max_dev_metric} "
                    f"({max_dev_pct:.1f}% from baseline)."
                )
            elif worst_dev_severity == "WARNING" or has_warning:
                conf += 0.15
                if max_dev_metric:
                    evidence.append(
                        f"Current WARNING deviation on {max_dev_metric} "
                        f"({max_dev_pct:.1f}% from baseline)."
                    )
                else:
                    evidence.append("Current behavior severity is WARNING.")

            for ev in events:
                conf = min(1.0, conf + 0.10)
                evidence.append(f"Operational event detected: {ev}.")

            for pat in patterns:
                evidence.append(f"Anomaly pattern detected: {pat}.")

            # Check if historical recurring causes corroborate this
            # current pattern (look for keyword overlap).
            corroborated_by = []
            for rc in recurring_causes:
                rc_cause = str(rc.get("cause", "")).lower()
                count = rc.get("count", 0)
                # Keyword overlap check (simple, deterministic)
                current_keywords = set(cause_label.lower().split())
                hist_keywords = set(rc_cause.split())
                overlap = current_keywords & hist_keywords
                meaningful_overlap = {
                    w for w in overlap
                    if len(w) > 3 and w not in
                    {"with", "from", "that", "this", "have", "been", "were"}
                }
                if meaningful_overlap or rc_cause in cause_label.lower():
                    corroborated_by.append(rc)

            if corroborated_by:
                best = max(corroborated_by, key=lambda r: r.get("count", 0))
                best_count = best.get("count", 0)
                if best_count >= 3:
                    conf = min(1.0, conf + 0.30)
                elif best_count >= 2:
                    conf = min(1.0, conf + 0.20)
                else:
                    conf = min(1.0, conf + 0.10)
                evidence.append(
                    f"Historical evidence suggests: '{best['cause']}' "
                    f"appeared in {best_count} similar incident"
                    f"{'s' if best_count != 1 else ''}."
                )
                source = "combined"
            else:
                source = "current"

            # ic_root_cause corroboration
            if ic_root_cause:
                ic_lower = ic_root_cause.lower()
                cause_lower = cause_label.lower()
                if any(w in cause_lower for w in ic_lower.split() if len(w) > 3):
                    conf = min(1.0, conf + 0.15)
                    evidence.append(
                        f"Lookup incident history also reports: '{ic_root_cause}'."
                    )
                    source = "combined"

            hypotheses.append({
                "cause": cause_label,
                "confidence": round(conf, 2),
                "confidence_level": _confidence_level(conf),
                "evidence": evidence,
                "source": source
            })

        # ── STEP 2: Historical-only hypothesis ─────────────────
        # Surfaces historical root causes that don't overlap with any
        # current hypothesis already generated.
        if recurring_causes:
            existing_causes = {
                h["cause"].lower() for h in hypotheses
            }
            for rc in recurring_causes:
                rc_cause = rc.get("cause", "")
                if not _is_valid(rc_cause):
                    continue
                # Skip if already captured by current hypothesis
                if any(rc_cause.lower() in ec for ec in existing_causes):
                    continue
                # Also skip if this is a standalone IC match we'll handle below
                if ic_root_cause and rc_cause.lower() == ic_root_cause.lower():
                    continue

                count = rc.get("count", 0)
                conf = 0.0
                if count >= 3:
                    conf += 0.30
                elif count >= 2:
                    conf += 0.20
                else:
                    conf += 0.10

                evidence = [
                    f"Historical evidence suggests '{rc_cause}' appeared in "
                    f"{count} similar incident"
                    f"{'s' if count != 1 else ''}."
                ]

                # Check ic corroboration for this historical cause
                if ic_root_cause:
                    if ic_root_cause.lower() == rc_cause.lower() or \
                       any(w in rc_cause.lower()
                           for w in ic_root_cause.lower().split() if len(w) > 3):
                        conf = min(1.0, conf + 0.15)
                        evidence.append(
                            f"Lookup incident history also reports: '{ic_root_cause}'."
                        )

                hypotheses.append({
                    "cause": rc_cause,
                    "confidence": round(conf, 2),
                    "confidence_level": _confidence_level(conf),
                    "evidence": evidence,
                    "source": "historical"
                })

        # ── STEP 3: incident_context-only hypothesis ────────────
        if ic_root_cause:
            # Only add if not already covered
            already_covered = any(
                ic_root_cause.lower() in h["cause"].lower()
                or h["cause"].lower() in ic_root_cause.lower()
                for h in hypotheses
            )
            if not already_covered:
                conf = 0.15  # points for ic match
                evidence = [
                    f"Lookup incident history reports root cause: '{ic_root_cause}'."
                ]
                hypotheses.append({
                    "cause": ic_root_cause,
                    "confidence": round(conf, 2),
                    "confidence_level": _confidence_level(conf),
                    "evidence": evidence,
                    "source": "historical"
                })

        if not hypotheses:
            return dict(_UNAVAILABLE)

        # ── Sort by confidence descending ─────────────────────
        hypotheses.sort(key=lambda h: h["confidence"], reverse=True)

        # ── Build explanation ─────────────────────────────────
        top = hypotheses[0]
        explanation_parts = [
            f"Phase 4 RCA identified {len(hypotheses)} "
            f"hypothesis/hypotheses based on available evidence."
        ]
        explanation_parts.append(
            f"Highest-confidence hypothesis (confidence={top['confidence']}, "
            f"level={top['confidence_level']}, source={top['source']}): "
            f"'{top['cause']}'."
        )
        if len(hypotheses) > 1:
            explanation_parts.append(
                "Additional hypotheses are listed in descending confidence order. "
                "All are labelled by evidence source (current/historical/combined)."
            )
        explanation_parts.append(
            "Note: Hypotheses labelled 'historical' or 'combined' incorporate "
            "historical evidence and must not be treated as confirmed current facts."
        )

        return {
            "available": True,
            "primary_hypotheses": hypotheses,
            "explanation": " ".join(explanation_parts)
        }
