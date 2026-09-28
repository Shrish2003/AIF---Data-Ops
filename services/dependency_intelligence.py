"""
==========================================================
Dependency Intelligence Service  — Phase 4.3
==========================================================

Responsibility:
    Extract upstream/downstream dependency information from
    the existing lineage_context that is already embedded inside
    every behavior_object (via the ContextEnricher pipeline stage).

    The current repository's pipeline_lineage.csv provides
    one-hop lineage per entity.  This service is honest about
    that limitation: transitive_impact is empty when deeper
    lineage data is unavailable, and impact_depth is set to 1.

    This service performs ZERO ChromaDB queries, ZERO Ollama
    calls, and ZERO CSV reads.  It operates on the lineage_context
    dict already in memory.

Input:
    lineage_context  — dict from entity.context.lineage_context
    entity_id        — str, the entity under analysis

Output:
    dependency_intelligence dict

IMPORTANT:
    Do NOT introduce a graph database, Neo4j, or any graph
    framework.  Use only the data already available in the
    existing lineage_context dict.
==========================================================
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_UNAVAILABLE = {
    "available": False,
    "upstream": [],
    "downstream": [],
    "dependency_type": "Not available",
    "direct_impact": [],
    "transitive_impact": [],
    "impact_depth": 0,
    "affected_count": 0,
    "note": "Lineage information not available for this entity."
}


def _is_valid(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip().lower()
    return s not in ("", "none", "null", "not available", "not determined",
                     "unknown", "n/a", "nan")


class DependencyIntelligenceService:
    """
    Extracts one-hop upstream/downstream dependency intelligence
    from the lineage_context dict already embedded in the
    operational entity.

    Honest about data limitations — transitive_impact is empty
    when only one-hop data is available.
    """

    # ----------------------------------------------------------
    # Public interface
    # ----------------------------------------------------------

    @staticmethod
    def analyze(
        lineage_context: Dict[str, Any],
        entity_id: str
    ) -> Dict[str, Any]:
        """
        Produce structured dependency intelligence.

        Parameters
        ----------
        lineage_context : dict
            From behavior_object["observation"]["entity"]["context"]["lineage_context"].
            Expected keys: entity_id, source_system, depends_on, downstream,
            dependency_type.
        entity_id : str
            The entity currently under analysis.

        Returns
        -------
        dict
            available, upstream, downstream, dependency_type,
            direct_impact, transitive_impact, impact_depth, affected_count
        """
        try:
            return DependencyIntelligenceService._analyze(
                lineage_context, entity_id
            )
        except Exception as exc:
            logger.warning(
                "DependencyIntelligenceService.analyze failed gracefully: %s", exc
            )
            return dict(_UNAVAILABLE)

    # ----------------------------------------------------------
    # Internal implementation
    # ----------------------------------------------------------

    @staticmethod
    def _analyze(
        lineage_context: Dict[str, Any],
        entity_id: str
    ) -> Dict[str, Any]:
        if not lineage_context:
            return dict(_UNAVAILABLE)

        eid = str(entity_id).strip()

        # ── Upstream (depends_on) ─────────────────────────────
        upstream: List[str] = []
        depends_on_raw = lineage_context.get("depends_on")
        if _is_valid(depends_on_raw):
            dep = str(depends_on_raw).strip()
            # Self-references are uninformative — skip them
            if dep != eid:
                upstream.append(dep)

        # ── Downstream ────────────────────────────────────────
        downstream: List[str] = []
        downstream_raw = lineage_context.get("downstream")
        if _is_valid(downstream_raw):
            down = str(downstream_raw).strip()
            if down != eid:
                downstream.append(down)

        # ── Dependency type ───────────────────────────────────
        dep_type_raw = lineage_context.get("dependency_type")
        dep_type = (
            str(dep_type_raw).strip()
            if _is_valid(dep_type_raw)
            else "Not available"
        )

        # ── Direct impact ─────────────────────────────────────
        # The entity under analysis directly depends on upstream
        # entities and itself provides data/services to downstream
        # entities.  Both sets form the direct impact surface.
        direct_impact: List[str] = list(set(upstream + downstream))

        # ── Transitive impact ─────────────────────────────────
        # Current data is one-hop only.  We cannot compute
        # transitive (multi-hop) impact without deeper lineage.
        # Honest limitation: return empty list.
        transitive_impact: List[str] = []

        # ── Impact depth ─────────────────────────────────────
        # 1 if we have any direct relationships, 0 otherwise.
        impact_depth = 1 if direct_impact else 0

        if not direct_impact and not upstream and not downstream:
            # lineage_context existed but carried no useful relationships
            return dict(_UNAVAILABLE)

        return {
            "available": True,
            "upstream": upstream,
            "downstream": downstream,
            "dependency_type": dep_type,
            "direct_impact": direct_impact,
            "transitive_impact": transitive_impact,
            "impact_depth": impact_depth,
            "affected_count": len(direct_impact),
            "note": (
                "Lineage data provides one-hop relationships only. "
                "Transitive impact requires deeper lineage data."
            )
        }
