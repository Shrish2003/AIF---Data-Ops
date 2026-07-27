import pytest
import os
import json
from unittest.mock import MagicMock, patch
from agents.recommendation.evaluation.golden_truth_repository import GoldenTruthRepository
from agents.recommendation.evaluation.scenario_resolver import ScenarioResolver
from agents.recommendation.evaluation.ragas_evaluator import RagasEvaluator, EvaluationBackendUnavailableError
from agents.recommendation.evaluation.evaluation_service import RecommendationEvaluationService

@pytest.fixture
def mock_scenarios():
    return [
        {
            "scenario_name": "Kafka High Lag Deviation",
            "context": {
                "platform": "Kafka",
                "execution_status": "FAILED",
                "behavior_severity": "WARNING",
                "risk_severity": "MEDIUM",
                "integrity_status": "PASS",
                "primary_failure_reason": "Record Count Validation"
            },
            "expected_recommendation": "Investigate Kafka record lag and quarantine affected partitions."
        },
        {
            "scenario_name": "Critical Airflow Timeout",
            "context": {
                "platform": "Airflow",
                "execution_status": "FAILED",
                "behavior_severity": "CRITICAL",
                "risk_severity": "CRITICAL"
            },
            "expected_recommendation": "Escalate task timeout immediately to on-call."
        },
        {
            "scenario_name": "Standard Low Priority Baseline",
            "context": {
                "behavior_severity": "NORMAL",
                "risk_severity": "LOW",
                "integrity_status": "PASS"
            },
            "expected_recommendation": "No action required. Monitoring is stable."
        }
    ]

# ----------------------------------------------------------------------
# 1. Golden Truth Repository Tests
# ----------------------------------------------------------------------
def test_repository_load_file_not_found():
    repo = GoldenTruthRepository(dataset_path="non_existent_file.json")
    scenarios = repo.get_all_scenarios()
    assert scenarios == []

def test_repository_load_valid_file(tmp_path, mock_scenarios):
    temp_file = tmp_path / "test_golden_truth.json"
    with open(temp_file, "w") as f:
        json.dump(mock_scenarios, f)
        
    repo = GoldenTruthRepository(dataset_path=str(temp_file))
    scenarios = repo.get_all_scenarios()
    assert len(scenarios) == 3
    assert scenarios[0]["scenario_name"] == "Kafka High Lag Deviation"

# ----------------------------------------------------------------------
# 2. Scenario Resolver Tests
# ----------------------------------------------------------------------
def test_resolver_exact_match(mock_scenarios):
    resolver = ScenarioResolver()
    
    # Context that matches Kafka High Lag Deviation
    context = {
        "platform": "Kafka",
        "execution_status": "FAILED",
        "behavior_severity": "WARNING",
        "risk_severity": "MEDIUM",
        "integrity_status": "PASS",
        "primary_failure_reason": "Record Count Validation",
        "extra_field": "unrelated"
    }
    
    resolved = resolver.resolve(context, mock_scenarios)
    assert resolved is not None
    assert resolved["scenario_name"] == "Kafka High Lag Deviation"
    assert resolved["expected_recommendation"] == "Investigate Kafka record lag and quarantine affected partitions."

def test_resolver_case_insensitivity(mock_scenarios):
    resolver = ScenarioResolver()
    
    context = {
        "platform": "kafka", # lowercase
        "execution_status": "failed", # lowercase
        "behavior_severity": "Warning", # Mixed case
        "risk_severity": "medium",
        "integrity_status": "pass",
        "primary_failure_reason": "record count validation"
    }
    
    resolved = resolver.resolve(context, mock_scenarios)
    assert resolved is not None
    assert resolved["scenario_name"] == "Kafka High Lag Deviation"

def test_resolver_specificity_scoring(mock_scenarios):
    resolver = ScenarioResolver()
    
    # This matches BOTH "Critical Airflow Timeout" (2 keys) AND "Standard Low Priority Baseline"
    # wait, baseline has behavior_severity: "NORMAL", timeout has behavior_severity: "CRITICAL".
    # Let's check Airflow timeout match keys: platform="Airflow", execution_status="FAILED", behavior_severity="CRITICAL", risk_severity="CRITICAL"
    # If we supply a context matching "Critical Airflow Timeout" but with platform Airflow.
    context = {
        "platform": "Airflow",
        "execution_status": "FAILED",
        "behavior_severity": "CRITICAL",
        "risk_severity": "CRITICAL",
        "integrity_status": "PASS" # standard baseline matches this, but Airflow timeout matches 4 keys.
    }
    
    resolved = resolver.resolve(context, mock_scenarios)
    assert resolved is not None
    assert resolved["scenario_name"] == "Critical Airflow Timeout" # higher match score (4 vs 0 or 1)

def test_resolver_no_match(mock_scenarios):
    resolver = ScenarioResolver()
    
    context = {
        "platform": "SAP",
        "execution_status": "SUCCESS",
        "behavior_severity": "CRITICAL",
        "risk_severity": "LOW"
    }
    
    resolved = resolver.resolve(context, mock_scenarios)
    assert resolved is None

# ----------------------------------------------------------------------
# 3. Ragas Evaluator Tests
# ----------------------------------------------------------------------
@patch("agents.recommendation.evaluation.ragas_evaluator.RAGAS_AVAILABLE", False)
def test_ragas_evaluator_library_unavailable():
    evaluator = RagasEvaluator()
    with pytest.raises(EvaluationBackendUnavailableError) as exc_info:
        evaluator.evaluate_correctness("rec", "expected")
    assert "package is not installed" in str(exc_info.value)

# ----------------------------------------------------------------------
# 4. Evaluation Service Tests
# ----------------------------------------------------------------------
def test_service_unresolved_scenario(tmp_path):
    # Setup dummy database with empty list
    temp_file = tmp_path / "empty_golden_truth.json"
    with open(temp_file, "w") as f:
        json.dump([], f)
        
    service = RecommendationEvaluationService(dataset_path=str(temp_file))
    
    base_rec = {
        "entity_id": "BRO0002",
        "recommendation": "Dummy generated recommendation"
    }
    
    final_rec = service.evaluate(base_rec, {"platform": "Kafka"})
    
    assert final_rec["evaluation"]["status"] == "UNAVAILABLE"
    assert "evaluation" in final_rec
    assert final_rec["evaluation"]["metrics"] == {}

@patch("agents.recommendation.evaluation.ragas_evaluator.RagasEvaluator.evaluate_correctness")
def test_service_successful_evaluation(mock_eval, tmp_path, mock_scenarios):
    mock_eval.return_value = 0.95
    
    temp_file = tmp_path / "valid_golden_truth.json"
    with open(temp_file, "w") as f:
        json.dump(mock_scenarios, f)
        
    service = RecommendationEvaluationService(dataset_path=str(temp_file))
    
    base_rec = {
        "entity_id": "BRO0001",
        "recommendation": "Investigate Kafka record lag and quarantine affected partitions."
    }
    
    context = {
        "platform": "Kafka",
        "execution_status": "FAILED",
        "behavior_severity": "WARNING",
        "risk_severity": "MEDIUM",
        "integrity_status": "PASS",
        "primary_failure_reason": "Record Count Validation"
    }
    
    final_rec = service.evaluate(base_rec, context)
    
    assert final_rec["evaluation"]["status"] == "COMPLETED"
    assert final_rec["evaluation"]["engine"] == "ragas"
    assert final_rec["evaluation"]["metrics"]["factual_correctness"] == 0.95

@patch("agents.recommendation.evaluation.ragas_evaluator.RagasEvaluator.evaluate_correctness")
def test_service_failed_evaluation_backend(mock_eval, tmp_path, mock_scenarios):
    mock_eval.side_effect = EvaluationBackendUnavailableError("Ragas execution failed: API key not valid")
    
    temp_file = tmp_path / "valid_golden_truth.json"
    with open(temp_file, "w") as f:
        json.dump(mock_scenarios, f)
        
    service = RecommendationEvaluationService(dataset_path=str(temp_file))
    
    base_rec = {
        "entity_id": "BRO0001",
        "recommendation": "Investigate Kafka record lag and quarantine affected partitions."
    }
    
    context = {
        "platform": "Kafka",
        "execution_status": "FAILED",
        "behavior_severity": "WARNING",
        "risk_severity": "MEDIUM",
        "integrity_status": "PASS",
        "primary_failure_reason": "Record Count Validation"
    }
    
    final_rec = service.evaluate(base_rec, context)
    
    assert final_rec["evaluation"]["status"] == "UNAVAILABLE"
    assert final_rec["evaluation"]["engine"] == "ragas"
    assert final_rec["evaluation"]["metrics"] == {}

