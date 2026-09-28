"""
==========================================================
Phase 4 Orchestrator  — Phase 4.6
==========================================================

Responsibility:
    Thin coordinator that:
    1. Extracts all required context from behavior_object.
    2. Calls each Phase 4 service with exactly the right input.
    3. Assembles the final phase4_intelligence dict.
    4. Returns it with full failure isolation per service.

STRICT RULES:
    - Does NOT read CSV files.
    - Does NOT access ChromaDB.
    - Does NOT call Ollama.
    - Does NOT contain RCA business logic.
    - Does NOT duplicate ContextEnricher logic.
    - Performs ZERO additional ChromaDB / Ollama / HTTP calls.

All data is already in memory inside the behavior_object:
    behavior_object["observation"]["entity"]["context"]  ← lookup contexts
    behavior_object["historical_context"]                ← Phase 3 matches
    behavior_object["behavior"]                          ← current telemetry

This module is the single entry point for all Phase 4 services.
==========================================================
"""

import logging
from typing import Any, Dict

from services.historical_intelligence import HistoricalIntelligenceService
from services.root_cause_service import RootCauseService
from services.dependency_intelligence import DependencyIntelligenceService
from services.business_impact_service import BusinessImpactService
from services.recommendation_evidence import RecommendationEvidenceService

logger = logging.getLogger(__name__)

_FAILED_INTELLIGENCE = {"available": False}


def _extract_entity_context(behavior_object: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the entity.context block from a behavior_object.
    Returns an empty dict if the path does not exist.
    """
    return (
        behavior_object
        .get("observation", {})
        .get("entity", {})
        .get("context", {})
    )


def compute_phase4_intelligence(
    behavior_object: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Entry point for Phase 4 intelligence computation.

    Receives the full behavior_object (which already contains the
    Phase 3 historical_context and the ContextEnricher-stamped
    entity.context block) and returns the assembled
    phase4_intelligence dict.

    Each service is called inside an independent try/except block.
    A failure in one service does not prevent the others from running.

    Parameters
    ----------
    behavior_object : dict
        Full behavior object produced by BehaviorAgent.analyze_observation()
        after Phase 3 historical memory retrieval.

    Returns
    -------
    dict
        {
            "available": bool,
            "historical_intelligence": {...},
            "root_cause_intelligence": {...},
            "dependency_intelligence": {...},
            "business_impact": {...},
            "recommendation_evidence": {...}
        }
    """
    # ── Extract contexts ────────────────────────────────────────
    entity_context = _extract_entity_context(behavior_object)
    historical_context = behavior_object.get("historical_context", {})
    entity_id = behavior_object.get("entity_id", "")

    business_context    = entity_context.get("business_context", {})
    incident_context    = entity_context.get("incident_context", {})
    lineage_context     = entity_context.get("lineage_context", {})
    recommendation_ctx  = entity_context.get("recommendation_context", {})

    # ── 1. Historical Intelligence ──────────────────────────────
    historical_intelligence: Dict[str, Any] = _FAILED_INTELLIGENCE.copy()
    try:
        historical_intelligence = HistoricalIntelligenceService.analyze(
            historical_context
        )
    except Exception as exc:
        logger.warning(
            "Phase 4 — historical_intelligence failed: %s", exc
        )

    # ── 2. Root Cause Intelligence ─────────────────────────────
    root_cause_intelligence: Dict[str, Any] = _FAILED_INTELLIGENCE.copy()
    try:
        root_cause_intelligence = RootCauseService.analyze(
            behavior_object,
            historical_intelligence,
            incident_context
        )
    except Exception as exc:
        logger.warning(
            "Phase 4 — root_cause_intelligence failed: %s", exc
        )

    # ── 3. Dependency Intelligence ─────────────────────────────
    dependency_intelligence: Dict[str, Any] = _FAILED_INTELLIGENCE.copy()
    try:
        dependency_intelligence = DependencyIntelligenceService.analyze(
            lineage_context,
            entity_id
        )
    except Exception as exc:
        logger.warning(
            "Phase 4 — dependency_intelligence failed: %s", exc
        )

    # ── 4. Business Impact ─────────────────────────────────────
    business_impact: Dict[str, Any] = _FAILED_INTELLIGENCE.copy()
    try:
        business_impact = BusinessImpactService.analyze(
            business_context,
            behavior_object
        )
    except Exception as exc:
        logger.warning(
            "Phase 4 — business_impact failed: %s", exc
        )

    # ── 5. Recommendation Evidence ─────────────────────────────
    recommendation_evidence: Dict[str, Any] = _FAILED_INTELLIGENCE.copy()
    try:
        recommendation_evidence = RecommendationEvidenceService.analyze(
            recommendation_ctx,
            incident_context,
            historical_context
        )
    except Exception as exc:
        logger.warning(
            "Phase 4 — recommendation_evidence failed: %s", exc
        )

    # ── Assemble phase4_intelligence ───────────────────────────
    any_available = any([
        historical_intelligence.get("available"),
        root_cause_intelligence.get("available"),
        dependency_intelligence.get("available"),
        business_impact.get("available"),
        recommendation_evidence.get("available")
    ])

    return {
        "available": any_available,
        "historical_intelligence": historical_intelligence,
        "root_cause_intelligence": root_cause_intelligence,
        "dependency_intelligence": dependency_intelligence,
        "business_impact": business_impact,
        "recommendation_evidence": recommendation_evidence
    }
