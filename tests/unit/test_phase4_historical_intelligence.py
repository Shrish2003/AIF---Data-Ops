"""
==========================================================
Phase 4 Unit Tests — Historical Intelligence, RCA,
Dependency, Business Impact, Recommendation Evidence
==========================================================

26 tests covering all Phase 4 services.

Tests operate on dict inputs — no file I/O, no ChromaDB,
no Ollama.  Services are imported directly; no mocking of
core logic under test.
==========================================================
"""

import pytest
from unittest.mock import patch, MagicMock

# ── Service imports ──────────────────────────────────────
from services.historical_intelligence import HistoricalIntelligenceService
from services.root_cause_service import RootCauseService
from services.dependency_intelligence import DependencyIntelligenceService
from services.business_impact_service import BusinessImpactService
from services.recommendation_evidence import RecommendationEvidenceService
from services.phase4_orchestrator import compute_phase4_intelligence

# ── Agent imports for integration tests ─────────────────
from agents.behavior.behavior_agent import BehaviorAgent
from agents.risk.risk_agent import RiskPredictionAgent


# ============================================================
# Shared Fixtures
# ============================================================

@pytest.fixture
def sample_matches():
    """Three normalised historical matches from OperationalMemoryRetriever."""
    return [
        {
            "memory_id": "mem_001",
            "incident_id": "INC001",
            "source_system": "Kafka",
            "source_dataset": "kafka",
            "incident_type": "Consumer Lag",
            "distance": 0.15,
            "root_cause": "Consumer processing delay",
            "recommendation": "Scale consumer group",
            "action_taken": "Increased consumer replicas",
            "outcome": "Resolved",
            "resolution_status": "RESOLVED"
        },
        {
            "memory_id": "mem_002",
            "incident_id": "INC002",
            "source_system": "Kafka",
            "source_dataset": "kafka",
            "incident_type": "Consumer Lag",
            "distance": 0.22,
            "root_cause": "Consumer processing delay",
            "recommendation": "Scale consumer group",
            "action_taken": "Increased consumer replicas",
            "outcome": "Resolved",
            "resolution_status": "RESOLVED"
        },
        {
            "memory_id": "mem_003",
            "incident_id": "INC003",
            "source_system": "Kafka",
            "source_dataset": "kafka",
            "incident_type": "Throughput Degradation",
            "distance": 0.38,
            "root_cause": "Broker overload",
            "recommendation": "Rebalance partitions",
            "action_taken": "Rebalanced partitions",
            "outcome": "Open",
            "resolution_status": "OPEN"
        }
    ]


@pytest.fixture
def historical_context_with_matches(sample_matches):
    return {
        "available": True,
        "retrieval_query": "Kafka consumer lag ...",
        "retrieval_count": 3,
        "matches": sample_matches
    }


@pytest.fixture
def historical_context_empty():
    return {
        "available": False,
        "retrieval_query": "",
        "retrieval_count": 0,
        "matches": []
    }


@pytest.fixture
def behavior_object_critical():
    return {
        "entity_id": "BRO0001",
        "entity_name": "billing.transactions",
        "entity_type": "Stream",
        "source_system": "Kafka",
        "observation": {
            "observations": {
                "events": ["Execution Failure"],
                "trend": {}
            },
            "entity": {
                "entity_id": "BRO0001",
                "attributes": {"elapsed_runtime": 100},
                "context": {
                    "business_context": {
                        "entity_id": "BRO0001",
                        "business_unit": "Human Resources",
                        "owner": "Michael Brown",
                        "criticality": "High",
                        "sla_minutes": 5,
                        "application": "Fraud Detection",
                        "cost_center": "HR-201"
                    },
                    "incident_context": {
                        "entity_id": "BRO0001",
                        "total_incidents": 9,
                        "root_cause": "Pod Crash",
                        "resolution": "Replace Sensor",
                        "severity": "Critical"
                    },
                    "lineage_context": {
                        "entity_id": "BRO0001",
                        "source_system": "Kafka",
                        "depends_on": "DAG0093",
                        "downstream": "BRO0001",
                        "dependency_type": "Processes"
                    },
                    "recommendation_context": {
                        "entity_id": "BRO0001",
                        "recommendation": "Scale Kubernetes Cluster",
                        "impact": "Improved Reliability",
                        "applied": "Yes",
                        "success_rate": 95
                    }
                }
            }
        },
        "behavior": {
            "severity": "CRITICAL",
            "patterns": ["Isolated Metric Anomaly"],
            "deviation": {
                "throughput": {
                    "severity": "CRITICAL",
                    "value": 607.0,
                    "baseline": 100.0,
                    "deviation_percent": 507.0
                }
            },
            "behavior_score": 72.0,
            "confidence": 0.82
        },
        "historical_context": {
            "available": False,
            "retrieval_count": 0,
            "matches": []
        }
    }


@pytest.fixture
def observation_object_for_agent():
    """Minimal observation object accepted by BehaviorAgent."""
    return {
        "entity_id": "MAC0008",
        "entity_name": "Packaging Line 1",
        "entity_type": "Machine",
        "source_system": "Manufacturing",
        "event_timestamp": "2026-07-25T22:09:00",
        "entity": {
            "entity_id": "MAC0008",
            "attributes": {"elapsed_runtime": 100, "retry_count": 0},
            "context": {
                "business_context": {
                    "criticality": "Critical",
                    "sla_minutes": 60,
                    "business_unit": "Quality Assurance",
                    "application": "Fraud Detection",
                    "owner": "David Wilson",
                    "cost_center": "SEC-701"
                },
                "incident_context": {"total_incidents": 2},
                "lineage_context": {
                    "entity_id": "MAC0008",
                    "source_system": "Manufacturing",
                    "depends_on": "JOB0913",
                    "downstream": "MAC0008",
                    "dependency_type": "Consumes Data"
                },
                "recommendation_context": {
                    "recommendation": "Inspect Machinery",
                    "impact": "Downtime Reduced",
                    "applied": "No",
                    "success_rate": 80
                }
            }
        },
        "observations": {
            "events": [],
            "trend": {}
        }
    }


# ============================================================
# 1. Historical Intelligence — Aggregation
# ============================================================

def test_historical_intelligence_aggregates_matches(historical_context_with_matches):
    result = HistoricalIntelligenceService.analyze(historical_context_with_matches)
    assert result["available"] is True
    assert result["match_count"] == 3


def test_historical_intelligence_recurring_root_causes(historical_context_with_matches):
    result = HistoricalIntelligenceService.analyze(historical_context_with_matches)
    causes = result["recurring_root_causes"]
    assert len(causes) >= 1
    # "Consumer processing delay" appears 2× — must be first
    assert causes[0]["cause"] == "Consumer processing delay"
    assert causes[0]["count"] == 2


def test_historical_intelligence_outcome_counts(historical_context_with_matches):
    result = HistoricalIntelligenceService.analyze(historical_context_with_matches)
    outcomes = result["historical_outcomes"]
    # 2 Resolved + 1 Open
    resolved_key = next(
        (k for k in outcomes if k.lower() in {"resolved", "closed", "fixed"}), None
    )
    assert resolved_key is not None
    assert outcomes[resolved_key] == 2


def test_historical_intelligence_empty_matches(historical_context_empty):
    result = HistoricalIntelligenceService.analyze(historical_context_empty)
    assert result["available"] is False
    assert result["match_count"] == 0
    assert result["recurring_root_causes"] == []


def test_historical_intelligence_disabled_memory():
    ctx = {"available": False, "retrieval_count": 0, "matches": []}
    result = HistoricalIntelligenceService.analyze(ctx)
    assert result["available"] is False


def test_historical_intelligence_graceful_on_malformed():
    # None value for matches — should not crash
    ctx = {"available": True, "matches": None}
    result = HistoricalIntelligenceService.analyze(ctx)
    assert result["available"] is False


# ============================================================
# 2. Root Cause Service — Hypotheses
# ============================================================

@pytest.fixture
def hist_intel_with_causes():
    return {
        "available": True,
        "match_count": 2,
        "recurring_root_causes": [
            {"cause": "Consumer processing delay", "count": 2}
        ],
        "recurring_symptoms": [],
        "historical_recommendations": [],
        "historical_outcomes": {},
        "successful_actions": [],
        "failed_actions": [],
        "summary": "Historical evidence suggests ..."
    }


def test_rca_current_only(behavior_object_critical, hist_intel_with_causes):
    # Remove history from intel to isolate current-only path
    hist_intel_no_match = {**hist_intel_with_causes, "available": False, "recurring_root_causes": []}
    result = RootCauseService.analyze(
        behavior_object_critical,
        hist_intel_no_match,
        {}
    )
    assert result["available"] is True
    hyp = result["primary_hypotheses"][0]
    # Source should be "current" when no historical corroboration
    assert hyp["source"] in ("current", "combined")


def test_rca_combined_evidence(behavior_object_critical, hist_intel_with_causes):
    result = RootCauseService.analyze(
        behavior_object_critical,
        hist_intel_with_causes,
        {"root_cause": "Consumer processing delay"}
    )
    assert result["available"] is True
    # At least one hypothesis
    assert len(result["primary_hypotheses"]) >= 1


def test_rca_historical_only():
    # No current severity anomaly, only historical
    beh_obj = {
        "entity_id": "X001",
        "behavior": {
            "severity": "NORMAL",
            "patterns": [],
            "deviation": {},
            "confidence": 0.6
        },
        "observation": {"observations": {"events": [], "trend": {}}},
        "historical_context": {"available": False, "matches": []}
    }
    hist_intel = {
        "available": True,
        "match_count": 3,
        "recurring_root_causes": [{"cause": "Network timeout", "count": 3}],
        "recurring_symptoms": [],
        "historical_recommendations": [],
        "historical_outcomes": {},
        "successful_actions": [],
        "failed_actions": [],
        "summary": ""
    }
    result = RootCauseService.analyze(beh_obj, hist_intel, {})
    assert result["available"] is True
    hyp = result["primary_hypotheses"][0]
    assert hyp["source"] == "historical"
    assert "Network timeout" in hyp["cause"]


def test_rca_no_fabrication():
    # Completely empty inputs — should return available=False
    beh_obj = {
        "entity_id": "X002",
        "behavior": {"severity": "NORMAL", "patterns": [], "deviation": {}, "confidence": 0.5},
        "observation": {"observations": {"events": [], "trend": {}}},
        "historical_context": {"available": False, "matches": []}
    }
    result = RootCauseService.analyze(beh_obj, {"available": False}, {})
    # With NORMAL severity, no events, no history — should have no hypotheses
    assert result["available"] is False or result["primary_hypotheses"] == []


def test_rca_confidence_is_deterministic(behavior_object_critical, hist_intel_with_causes):
    result_a = RootCauseService.analyze(
        behavior_object_critical, hist_intel_with_causes, {}
    )
    result_b = RootCauseService.analyze(
        behavior_object_critical, hist_intel_with_causes, {}
    )
    # Same inputs → same confidence
    conf_a = [h["confidence"] for h in result_a["primary_hypotheses"]]
    conf_b = [h["confidence"] for h in result_b["primary_hypotheses"]]
    assert conf_a == conf_b


def test_rca_source_separation(behavior_object_critical, hist_intel_with_causes):
    result = RootCauseService.analyze(
        behavior_object_critical, hist_intel_with_causes, {}
    )
    for hyp in result["primary_hypotheses"]:
        assert hyp["source"] in ("current", "historical", "combined")


def test_rca_historical_not_claimed_as_current_fact():
    # Historical root cause must not be labelled source="current"
    beh_obj = {
        "entity_id": "X003",
        "behavior": {"severity": "NORMAL", "patterns": [], "deviation": {}, "confidence": 0.5},
        "observation": {"observations": {"events": [], "trend": {}}},
        "historical_context": {"available": False, "matches": []}
    }
    hist_intel = {
        "available": True,
        "match_count": 1,
        "recurring_root_causes": [{"cause": "Memory leak", "count": 1}],
        "recurring_symptoms": [],
        "historical_recommendations": [],
        "historical_outcomes": {},
        "successful_actions": [],
        "failed_actions": [],
        "summary": ""
    }
    result = RootCauseService.analyze(beh_obj, hist_intel, {})
    for hyp in result.get("primary_hypotheses", []):
        # A historical cause must not claim source="current"
        if "Memory leak" in hyp["cause"]:
            assert hyp["source"] != "current", (
                "Historical root cause incorrectly labelled as 'current'."
            )


# ============================================================
# 3. Dependency Intelligence
# ============================================================

def test_dependency_upstream_extracted():
    lineage = {
        "entity_id": "BRO0001",
        "source_system": "Kafka",
        "depends_on": "DAG0093",
        "downstream": "BRO0001",
        "dependency_type": "Processes"
    }
    result = DependencyIntelligenceService.analyze(lineage, "BRO0001")
    assert result["available"] is True
    assert "DAG0093" in result["upstream"]


def test_dependency_downstream_extracted():
    lineage = {
        "entity_id": "BRO0001",
        "source_system": "Kafka",
        "depends_on": "DAG0093",
        "downstream": "SVC_ABC",
        "dependency_type": "Processes"
    }
    result = DependencyIntelligenceService.analyze(lineage, "BRO0001")
    assert result["available"] is True
    assert "SVC_ABC" in result["downstream"]


def test_dependency_missing_lineage():
    result = DependencyIntelligenceService.analyze({}, "BRO0001")
    assert result["available"] is False


def test_dependency_depth_is_honest():
    # One-hop only: depth must be 1, transitive_impact must be empty
    lineage = {
        "entity_id": "BRO0001",
        "source_system": "Kafka",
        "depends_on": "DAG0093",
        "downstream": "BRO0001",
        "dependency_type": "Processes"
    }
    result = DependencyIntelligenceService.analyze(lineage, "BRO0001")
    assert result["impact_depth"] == 1
    assert result["transitive_impact"] == []


def test_dependency_self_reference_excluded():
    # depends_on == entity_id should not appear in upstream
    lineage = {
        "entity_id": "DAG0001",
        "depends_on": "DAG0001",
        "downstream": "DAG0001",
        "dependency_type": "Self"
    }
    result = DependencyIntelligenceService.analyze(lineage, "DAG0001")
    # Self-references excluded → no meaningful relationships → available=False
    assert result["available"] is False or result["upstream"] == []


# ============================================================
# 4. Business Impact
# ============================================================

def test_business_impact_maps_from_lookup():
    biz_ctx = {
        "business_unit": "Human Resources",
        "application": "Fraud Detection",
        "criticality": "High",
        "sla_minutes": 5,
        "owner": "Michael Brown",
        "cost_center": "HR-201"
    }
    beh_obj = {"behavior": {"severity": "CRITICAL"}, "historical_context": {"available": False}}
    result = BusinessImpactService.analyze(biz_ctx, beh_obj)
    assert result["available"] is True
    assert result["criticality"] == "High"
    assert result["business_unit"] == "Human Resources"


def test_business_criticality_comes_from_lookup_not_risk():
    # Even if behavior severity is CRITICAL, criticality must reflect lookup value
    biz_ctx = {"criticality": "Low", "sla_minutes": 120, "application": "App"}
    beh_obj = {"behavior": {"severity": "CRITICAL"}, "historical_context": {"available": False}}
    result = BusinessImpactService.analyze(biz_ctx, beh_obj)
    assert result["available"] is True
    # Criticality from lookup = "Low"
    assert result["criticality"] == "Low"
    # Impact level from matrix: Low × CRITICAL = MEDIUM
    assert result["impact_level"] == "MEDIUM"


def test_business_impact_missing_context():
    result = BusinessImpactService.analyze({}, {"behavior": {"severity": "CRITICAL"}, "historical_context": {}})
    assert result["available"] is False


# ============================================================
# 5. Recommendation Evidence
# ============================================================

def test_recommendation_evidence_success_rate():
    rec_ctx = {
        "recommendation": "Scale Kubernetes Cluster",
        "impact": "Improved Reliability",
        "applied": "Yes",
        "success_rate": 95
    }
    result = RecommendationEvidenceService.analyze(rec_ctx, {}, {"available": False, "matches": []})
    assert result["available"] is True
    assert result["success_rate"] == 95
    assert result["applied_previously"] is True


def test_recommendation_evidence_from_historical_matches(historical_context_with_matches):
    result = RecommendationEvidenceService.analyze(
        {},
        {},
        historical_context_with_matches
    )
    assert result["available"] is True
    assert result["historical_usage_count"] == 3
    # 2 resolved, 1 open
    assert result["successful_count"] == 2


def test_recommendation_evidence_missing_context():
    result = RecommendationEvidenceService.analyze({}, {}, {"available": False, "matches": []})
    assert result["available"] is False


def test_recommendation_evidence_no_guarantee_language():
    rec_ctx = {"recommendation": "Fix X", "applied": "Yes", "success_rate": 100}
    result = RecommendationEvidenceService.analyze(
        rec_ctx, {}, {"available": False, "matches": []}
    )
    # Summary must not contain "will solve" or "guaranteed"
    summary = result.get("evidence_summary", "")
    assert "will solve" not in summary.lower()
    assert "guaranteed" not in summary.lower()


# ============================================================
# 6. Phase 4 Integration — BehaviorAgent + RiskAgent
# ============================================================

def test_phase4_intelligence_added_to_behavior(observation_object_for_agent):
    with patch("services.memory_retriever.is_memory_enabled", return_value=False):
        agent = BehaviorAgent("config/behavior_rules.yaml")
        beh = agent.analyze_observation(observation_object_for_agent)
    assert "phase4_intelligence" in beh


def test_phase4_intelligence_propagates_to_risk(observation_object_for_agent):
    with patch("services.memory_retriever.is_memory_enabled", return_value=False):
        agent = BehaviorAgent("config/behavior_rules.yaml")
        beh = agent.analyze_observation(observation_object_for_agent)
        risk_agent = RiskPredictionAgent("config/risk_rules.yaml")
        risk = risk_agent.predict_risk(beh)
    assert "phase4_intelligence" in risk


def test_risk_score_invariance_with_phase4(observation_object_for_agent):
    """Risk score must be identical with and without phase4_intelligence."""
    risk_agent = RiskPredictionAgent("config/risk_rules.yaml")

    # Run A: memory OFF (no phase4 history)
    with patch("services.memory_retriever.is_memory_enabled", return_value=False):
        agent = BehaviorAgent("config/behavior_rules.yaml")
        beh_a = agent.analyze_observation(observation_object_for_agent)
    risk_a = risk_agent.predict_risk(beh_a)

    # Run B: memory enabled with mock historical context + phase4
    mock_ctx = {
        "available": True,
        "retrieval_query": "mock",
        "retrieval_count": 2,
        "matches": [
            {
                "memory_id": "mem_x",
                "incident_id": "INC-X",
                "root_cause": "test cause",
                "recommendation": "test rec",
                "action_taken": "did something",
                "outcome": "Resolved",
                "resolution_status": "RESOLVED",
                "incident_type": "Test"
            }
        ]
    }
    with patch("services.memory_retriever.is_memory_enabled", return_value=True), \
         patch(
             "services.memory_retriever.OperationalMemoryRetriever.retrieve_historical_evidence",
             return_value=mock_ctx
         ):
        agent2 = BehaviorAgent("config/behavior_rules.yaml")
        beh_b = agent2.analyze_observation(observation_object_for_agent)
    risk_b = risk_agent.predict_risk(beh_b)

    assert risk_a["risk_score"] == risk_b["risk_score"]
    assert risk_a["risk_severity"] == risk_b["risk_severity"]
    assert risk_a["prediction_confidence"] == risk_b["prediction_confidence"]


def test_backward_compatibility(observation_object_for_agent):
    """All pre-existing behavior keys must still be present."""
    with patch("services.memory_retriever.is_memory_enabled", return_value=False):
        agent = BehaviorAgent("config/behavior_rules.yaml")
        beh = agent.analyze_observation(observation_object_for_agent)

    # Pre-existing top-level keys
    for key in ("entity_id", "entity_name", "entity_type", "source_system",
                "event_timestamp", "observation", "behavior", "metadata",
                "historical_context"):
        assert key in beh, f"Pre-existing key '{key}' missing from behavior_object"


def test_phase4_failure_isolation(observation_object_for_agent):
    """If Phase 4 orchestrator raises, pipeline still returns a behavior object."""
    with patch("services.memory_retriever.is_memory_enabled", return_value=False), \
         patch(
             "services.phase4_orchestrator.compute_phase4_intelligence",
             side_effect=RuntimeError("Simulated Phase 4 crash")
         ):
        agent = BehaviorAgent("config/behavior_rules.yaml")
        beh = agent.analyze_observation(observation_object_for_agent)

    # Pipeline must not crash
    assert "entity_id" in beh
    # phase4_intelligence must be the fallback dict
    assert beh.get("phase4_intelligence", {}).get("available") is False
