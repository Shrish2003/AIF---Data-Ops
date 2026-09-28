"""
==========================================================
Business Impact Service  — Phase 4.4
==========================================================

Responsibility:
    Produce a structured business impact assessment from the
    business_context already embedded in every operational entity
    (via the ContextEnricher pipeline stage).

    CRITICAL RULES:
    ─────────────────────────────────────────────────────
    1. criticality MUST come from business_context.criticality.
       Never infer it from risk_score or behavior severity.
    2. impact_level is derived from a documented deterministic
       rule combining business criticality and current behavior
       severity.
    3. If required information is unavailable, return
       available = False.  Never fabricate values.
    ─────────────────────────────────────────────────────

IMPACT LEVEL RULE (deterministic):
    Inputs: business criticality (from lookup) × behavior severity
    Matrix:
                          Behavior Severity
    Business Criticality  CRITICAL  WARNING   NORMAL
    ──────────────────────────────────────────────────
    Critical              CRITICAL  HIGH      MEDIUM
    High                  HIGH      HIGH      MEDIUM
    Medium                HIGH      MEDIUM    LOW
    Low                   MEDIUM    LOW       LOW
    (unknown/missing)     Use behavior severity only (downgraded one tier)

This service performs ZERO ChromaDB queries, ZERO Ollama calls,
and ZERO CSV reads.
==========================================================
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_UNAVAILABLE = {
    "available": False,
    "business_unit": "Not available",
    "application": "Not available",
    "owner": "Not available",
    "cost_center": "Not available",
    "criticality": "Not available",
    "sla_minutes": None,
    "impact_level": "Not available",
    "affected_services": [],
    "reason": "Business context information not available."
}

# Documented impact level matrix
_IMPACT_MATRIX: Dict[str, Dict[str, str]] = {
    "critical": {"CRITICAL": "CRITICAL", "WARNING": "HIGH",   "NORMAL": "MEDIUM"},
    "high":     {"CRITICAL": "HIGH",     "WARNING": "HIGH",   "NORMAL": "MEDIUM"},
    "medium":   {"CRITICAL": "HIGH",     "WARNING": "MEDIUM", "NORMAL": "LOW"},
    "low":      {"CRITICAL": "MEDIUM",   "WARNING": "LOW",    "NORMAL": "LOW"},
}

# Downgrade map used when business criticality is unknown
_SEVERITY_DOWNGRADE: Dict[str, str] = {
    "CRITICAL": "HIGH",
    "WARNING": "MEDIUM",
    "NORMAL": "LOW"
}


def _is_valid(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip().lower()
    return s not in ("", "none", "null", "not available", "not determined",
                     "unknown", "n/a", "nan")


def _safe_str(value: Any, default: str = "Not available") -> str:
    return str(value).strip() if _is_valid(value) else default


class BusinessImpactService:
    """
    Maps existing business_context + current behavior severity into a
    structured business impact assessment.

    Criticality always comes from the lookup data — it is never
    inferred from technical severity signals.
    """

    # ----------------------------------------------------------
    # Public interface
    # ----------------------------------------------------------

    @staticmethod
    def analyze(
        business_context: Dict[str, Any],
        behavior_object: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Produce structured business impact.

        Parameters
        ----------
        business_context : dict
            From behavior_object["observation"]["entity"]["context"]["business_context"].
        behavior_object : dict
            Full behavior object (used only for behavior severity — never for
            criticality inference).

        Returns
        -------
        dict
            available, business_unit, application, owner, cost_center,
            criticality, sla_minutes, impact_level, affected_services, reason
        """
        try:
            return BusinessImpactService._analyze(
                business_context, behavior_object
            )
        except Exception as exc:
            logger.warning(
                "BusinessImpactService.analyze failed gracefully: %s", exc
            )
            return dict(_UNAVAILABLE)

    # ----------------------------------------------------------
    # Internal implementation
    # ----------------------------------------------------------

    @staticmethod
    def _analyze(
        business_context: Dict[str, Any],
        behavior_object: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not business_context:
            return dict(_UNAVAILABLE)

        # ── Extract business fields ────────────────────────────
        business_unit = _safe_str(business_context.get("business_unit"))
        application   = _safe_str(business_context.get("application"))
        owner         = _safe_str(business_context.get("owner"))
        cost_center   = _safe_str(business_context.get("cost_center"))
        criticality   = _safe_str(business_context.get("criticality"))

        # sla_minutes may be int or string
        sla_raw = business_context.get("sla_minutes")
        try:
            sla_minutes: Optional[int] = int(sla_raw) if sla_raw is not None else None
        except (ValueError, TypeError):
            sla_minutes = None

        # Must have at least one meaningful business field
        has_data = any(
            v != "Not available"
            for v in [business_unit, application, owner, criticality]
        )
        if not has_data:
            return dict(_UNAVAILABLE)

        # ── Current behavior severity ─────────────────────────
        # Used ONLY for impact_level matrix lookup — not for criticality.
        behavior_severity = (
            behavior_object.get("behavior", {}).get("severity", "NORMAL")
            or "NORMAL"
        ).upper()
        if behavior_severity not in ("CRITICAL", "WARNING", "NORMAL"):
            behavior_severity = "NORMAL"

        # ── Impact level (documented matrix) ──────────────────
        crit_lower = criticality.lower()
        if crit_lower in _IMPACT_MATRIX:
            impact_level = _IMPACT_MATRIX[crit_lower].get(
                behavior_severity, "MEDIUM"
            )
        else:
            # Unknown criticality — downgrade behavior severity one tier
            impact_level = _SEVERITY_DOWNGRADE.get(behavior_severity, "LOW")

        # ── Affected services from historical matches ──────────
        # Collect affected_services strings from historical context
        # stored in behavior_object (already retrieved — no new query).
        affected_services: List[str] = []
        hist_ctx = behavior_object.get("historical_context", {})
        if isinstance(hist_ctx, dict) and hist_ctx.get("available"):
            for match in (hist_ctx.get("matches") or []):
                svc_raw = match.get("metadata", {}).get("affected_services") \
                    if isinstance(match.get("metadata"), dict) else None
                if not svc_raw:
                    # Flat metadata stored as comma-separated string in retriever
                    svc_raw = match.get("affected_services")
                if _is_valid(svc_raw):
                    for svc in str(svc_raw).split(","):
                        svc = svc.strip()
                        if svc and svc not in affected_services:
                            affected_services.append(svc)

        # ── Reason narrative ───────────────────────────────────
        reason_parts = []
        if criticality != "Not available":
            reason_parts.append(
                f"Business context reports criticality: {criticality}."
            )
        if sla_minutes is not None:
            reason_parts.append(f"SLA is {sla_minutes} minutes.")
        if behavior_severity != "NORMAL":
            reason_parts.append(
                f"Current behavior severity is {behavior_severity}."
            )
        reason_parts.append(
            f"Derived impact level: {impact_level} "
            f"(criticality × behavior severity matrix)."
        )
        if affected_services:
            reason_parts.append(
                f"Historically affected services: "
                f"{', '.join(affected_services[:5])}."
            )

        return {
            "available": True,
            "business_unit": business_unit,
            "application": application,
            "owner": owner,
            "cost_center": cost_center,
            "criticality": criticality,
            "sla_minutes": sla_minutes,
            "impact_level": impact_level,
            "affected_services": affected_services,
            "reason": " ".join(reason_parts)
        }
