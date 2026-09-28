import os
import pytest
from unittest.mock import MagicMock, patch
from services.memory_retriever import OperationalMemoryRetriever, is_memory_enabled, get_top_k, get_distance_threshold
from agents.behavior.behavior_agent import BehaviorAgent
from agents.risk.risk_agent import RiskPredictionAgent

@pytest.fixture
def mock_behavior_object():
    return {
        "entity_id": "BRO0001",
        "entity_name": "billing.transactions",
        "entity_type": "Stream",
        "source_system": "Kafka",
        "observation": {
            "observations": {
                "events": ["Execution Failure"]
            }
        },
        "behavior": {
            "severity": "WARNING",
            "patterns": ["Isolated Metric Anomaly"],
            "deviation": {
                "throughput": {
                    "severity": "WARNING",
                    "value": 120.0,
                    "baseline": 150.0,
                    "deviation_percent": -20.0
                }
            },
            "behavior_score": 40.0,
            "confidence": 0.80
        }
    }

def test_retrieval_query_construction(mock_behavior_object):
    # Test query builder constructs a descriptive query from telemetry fields
    query = OperationalMemoryRetriever.build_retrieval_query(mock_behavior_object)
    assert "Incident on Kafka Stream 'billing.transactions' (ID: BRO0001)" in query
    assert "Behavior status is WARNING" in query
    assert "Anomaly patterns: Isolated Metric Anomaly" in query
    assert "throughput deviated by -20.0%" in query
    assert "Trigger events: Execution Failure" in query

def test_feature_flag_toggle(mock_behavior_object):
    # Test that setting flag to false disables retrieval completely
    with patch("services.memory_retriever.is_memory_enabled", return_value=False):
        res = OperationalMemoryRetriever.retrieve_historical_evidence(mock_behavior_object)
        assert res["available"] is False
        assert res["matches"] == []

def test_distance_threshold_filtering(mock_behavior_object):
    # Test that matches exceeding distance threshold are filtered out
    mock_results = [
        {"memory_id": "mem_1", "distance": 0.2, "metadata": {"incident_id": "INC1"}},
        {"memory_id": "mem_2", "distance": 0.6, "metadata": {"incident_id": "INC2"}}  # exceeds default 0.50
    ]
    with patch("services.memory_retriever.is_memory_enabled", return_value=True), \
         patch("services.memory_retriever.MemoryService") as mock_service_class:
        
        mock_instance = MagicMock()
        mock_instance.is_available = True
        mock_instance.search_similar_memories.return_value = mock_results
        mock_service_class.return_value = mock_instance
        
        res = OperationalMemoryRetriever.retrieve_historical_evidence(mock_behavior_object)
        assert res["available"] is True
        assert res["retrieval_count"] == 1
        assert res["matches"][0]["memory_id"] == "mem_1"

def test_graceful_degradation_offline_db(mock_behavior_object):
    # Test that retrieval fails gracefully and doesn't crash behavior agent if database is offline
    with patch("services.memory_retriever.is_memory_enabled", return_value=True), \
         patch("services.memory_retriever.MemoryService") as mock_service_class:
        
        mock_instance = MagicMock()
        mock_instance.is_available = False
        mock_service_class.return_value = mock_instance
        
        res = OperationalMemoryRetriever.retrieve_historical_evidence(mock_behavior_object)
        assert res["available"] is False
        assert res["matches"] == []

def test_propagation_and_risk_score_invariance():
    # Test that historical context propagates to behavior and risk objects without altering risk calculations
    obs_record = {
        "entity_id": "MAC0008",
        "entity_name": "Packaging Line 1",
        "entity_type": "Machine",
        "source_system": "Manufacturing",
        "event_timestamp": "2026-07-25T22:09:00",
        "entity": {
            "entity_id": "MAC0008",
            "attributes": {"elapsed_runtime": 100, "retry_count": 0},
            "context": {
                "business_context": {"criticality": "Critical", "sla_minutes": 180},
                "incident_context": {"total_incidents": 2}
            }
        },
        "observations": {
            "events": [],
            "trend": {}
        }
    }
    
    # Run with memory disabled
    with patch("services.memory_retriever.is_memory_enabled", return_value=False):
        behavior_agent = BehaviorAgent("config/behavior_rules.yaml")
        behavior_a = behavior_agent.analyze_observation(obs_record)
        
        risk_agent = RiskPredictionAgent("config/risk_rules.yaml")
        risk_a = risk_agent.predict_risk(behavior_a)
        
        assert behavior_a.get("historical_context", {}).get("available") is False
        assert risk_a.get("historical_context", {}).get("available") is False
        
    # Run with memory enabled (using mock retrieval matches)
    mock_context = {
        "available": True,
        "retrieval_query": "mock query",
        "retrieval_count": 1,
        "matches": [{"memory_id": "mem_123", "distance": 0.1, "incident_id": "INC-08"}]
    }
    with patch("services.memory_retriever.is_memory_enabled", return_value=True), \
         patch("services.memory_retriever.OperationalMemoryRetriever.retrieve_historical_evidence", return_value=mock_context):
        
        behavior_b = behavior_agent.analyze_observation(obs_record)
        risk_b = risk_agent.predict_risk(behavior_b)
        
        # Verify propagation
        assert behavior_b.get("historical_context", {}).get("available") is True
        assert risk_b.get("historical_context", {}).get("matches")[0]["memory_id"] == "mem_123"
        
        # Verify complete risk invariance
        assert risk_a["risk_score"] == risk_b["risk_score"]
        assert risk_a["risk_severity"] == risk_b["risk_severity"]
        assert risk_a["prediction_confidence"] == risk_b["prediction_confidence"]
