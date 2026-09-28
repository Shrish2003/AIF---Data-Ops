import os
import json
import pytest
from unittest.mock import patch, MagicMock
import requests

from services.memory_service import MemoryService
from services.memory_ingestion import (
    OperationalIncidentExtractor,
    get_lookup_context,
    ingest_real_operational_data
)

# ==========================================================
# Test Data Fixtures
# ==========================================================
@pytest.fixture
def sample_lookup_data():
    return {
        "business_context": [
            {
                "entity_id": "DAG0137",
                "source_system": "Airflow",
                "business_unit": "Billing",
                "criticality": "Critical",
                "application": "Customer Billing",
                "cost_center": "FIN-101"
            },
            {
                "entity_id": "BRO0001",
                "source_system": "Kafka",
                "business_unit": "DataOps",
                "criticality": "High",
                "application": "Message Streaming",
                "cost_center": "OPS-202"
            }
        ],
        "incident_history": [
            {
                "entity_id": "DAG0137",
                "source_system": "Airflow",
                "total_incidents": 5,
                "last_incident": "2026-07-10",
                "root_cause": "Network Lockout",
                "resolution": "Increased Timeout Limit",
                "severity": "High"
            }
        ],
        "recommendation_history": [
            {
                "entity_id": "DAG0137",
                "source_system": "Airflow",
                "recommendation": "Increase Timeout",
                "impact": "Runtime Reduced",
                "applied": "Yes",
                "success_rate": 90.0
            }
        ],
        "pipeline_lineage": [
            {
                "entity_id": "DAG0137",
                "source_system": "Airflow",
                "depends_on": "DAG0100",
                "downstream": "DAG0150, DAG0160",
                "dependency_type": "Hard"
            }
        ]
    }

@pytest.fixture
def sample_airflow_failed_entity():
    return {
        "entity_id": "RUN000001",
        "entity_name": "billing_pipeline",
        "entity_type": "Pipeline",
        "source_system": "Airflow",
        "execution_status": "FAILED",
        "event_timestamp": "2026-08-22T12:00:00Z",
        "attributes": {
            "dag_id": "DAG0137",
            "run_id": "RUN000001",
            "duration_seconds": 120,
            "try_number": 1
        },
        "dataset_name": "airflow"
    }

@pytest.fixture
def sample_kafka_anomaly_entity():
    return {
        "entity_id": "BRO0001",
        "entity_name": "events_topic",
        "entity_type": "Stream",
        "source_system": "Kafka",
        "execution_status": "SUCCESS",
        "event_timestamp": "2026-08-22T12:05:00Z",
        "attributes": {
            "broker_id": "BRO0001",
            "topic_name": "transaction.events",
            "lag": 45000,
            "throughput_msg_sec": 100
        },
        "dataset_name": "kafka"
    }

@pytest.fixture
def temp_db_dir(tmp_path):
    return str(tmp_path / "chromadb_test_phase2")


# ==========================================================
# Unit Tests
# ==========================================================
class TestOperationalMemoryPhase2:

    # 1. Test Incident Qualification Criteria
    def test_incident_qualification(self, sample_lookup_data):
        extractor = OperationalIncidentExtractor(sample_lookup_data)
        
        # Scenario A: Failed pipeline status must qualify
        entity_failed = {"execution_status": "FAILED"}
        obs_empty = {"observations": {}}
        beh_empty = {}
        risk_empty = {}
        integ_empty = {}
        
        is_incident, trigger = extractor.qualify_incident(
            entity_failed, obs_empty, beh_empty, risk_empty, integ_empty
        )
        assert is_incident is True
        assert trigger == "execution_status_failure"
        
        # Scenario B: Normal success without anomalies must NOT qualify (even if historical incident history exists)
        entity_success = {
            "execution_status": "SUCCESS",
            "context": {
                "incident_context": {
                    "total_incidents": 5  # Refinement 1: historical context is NOT a standalone trigger
                }
            }
        }
        is_incident_sec, trigger_sec = extractor.qualify_incident(
            entity_success, obs_empty, beh_empty, risk_empty, integ_empty
        )
        assert is_incident_sec is False
        assert trigger_sec is None
        
        # Scenario C: Active behavior anomaly triggers qualify
        entity_success_beh = {"execution_status": "SUCCESS"}
        beh_anomaly = {"behavior": {"severity": "CRITICAL"}}
        is_incident_beh, trigger_beh = extractor.qualify_incident(
            entity_success_beh, obs_empty, beh_anomaly, risk_empty, integ_empty
        )
        assert is_incident_beh is True
        assert trigger_beh == "behavior_severity_anomaly"

    # 2. Test lookup re-joins mapping (mismatched ID keys)
    def test_lookup_key_mapping(self, sample_lookup_data, sample_airflow_failed_entity, sample_kafka_anomaly_entity):
        # Scenario A: Airflow RUN000001 maps to lookup DAG0137 via dag_id attribute
        ctx_airflow = get_lookup_context("Airflow", sample_airflow_failed_entity, sample_lookup_data)
        assert ctx_airflow.get("business_context", {}).get("business_unit") == "Billing"
        assert ctx_airflow.get("incident_context", {}).get("root_cause") == "Network Lockout"
        
        # Scenario B: Kafka BRO0001 maps directly via entity_id
        ctx_kafka = get_lookup_context("Kafka", sample_kafka_anomaly_entity, sample_lookup_data)
        assert ctx_kafka.get("business_context", {}).get("business_unit") == "DataOps"
        
        # Scenario C: ADF pipelineRunId reformatting PIP000001 -> PIP0001
        adf_entity = {
            "entity_id": "PIP000001",
            "attributes": {"pipelineRunId": "PIP000001"}
        }
        adf_lookup = {
            "business_context": [{"entity_id": "PIP0001", "business_unit": "CloudOps"}]
        }
        ctx_adf = get_lookup_context("Azure Data Factory", adf_entity, adf_lookup)
        assert ctx_adf.get("business_context", {}).get("business_unit") == "CloudOps"

    # 3. Test extraction mappings, deterministic IDs, and no fabrication
    def test_extract_record_structure(self, sample_lookup_data, sample_airflow_failed_entity):
        extractor = OperationalIncidentExtractor(sample_lookup_data)
        obs = {"observations": {}}
        beh = {"behavior": {"severity": "CRITICAL"}}
        risk = {"risk_severity": "HIGH", "risk_score": 75.0, "risk_category": "Runtime Delay"}
        integ = {"integrity_status": "PASS"}
        rec = {"recommendation": "Scale cluster", "reason": "High execution time"}
        
        record = extractor.extract_record(
            sample_airflow_failed_entity, obs, beh, risk, integ, rec
        )
        
        # Deterministic IDs
        assert record["memory_id"].startswith("mem_")
        assert record["incident_id"] == "RUN000001"
        assert record["pipeline_id"] == "DAG0137"
        
        # Null handling (No fabrication)
        assert record["root_cause"] == "Network Lockout"  # Loaded from lookup
        assert record["resolution_status"] == "PENDING"   # FAILED execution state
        
        # Verify traceability fields separate from schema
        assert record["source_dataset"] == "airflow"
        assert record["source_record_id"] == "RUN000001"
        assert record["source_pipeline_id"] == "DAG0137"
        assert "created_at" in record
        assert record["memory_version"] == "2.0"

    # 4. Test Ingestion Idempotency & Separation of real vs test collections
    @patch('requests.post')
    def test_ingestion_idempotency_and_isolation(self, mock_post, temp_db_dir, sample_lookup_data, sample_airflow_failed_entity, tmp_path):
        # Mock embeddings
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": [0.01] * 128}
        mock_post.return_value = mock_response
        
        mock_output = {
            "adapter_output": {
                "parsed_lookup": sample_lookup_data
            },
            "operational_entity": {
                "airflow": [sample_airflow_failed_entity]
            },
            "observer_output": {"airflow": [{"entity_id": "RUN000001", "observations": {}}]},
            "behavior_output": {"airflow": [{"entity_id": "RUN000001", "behavior": {"severity": "CRITICAL"}}]},
            "risk_output": {"airflow": [{"entity_id": "RUN000001", "risk_severity": "HIGH", "risk_score": 75.0}]},
            "integrity_output": {"airflow": [{"entity_id": "RUN000001", "integrity_status": "PASS"}]},
            "recommendation_output": {"airflow": [{"entity_id": "RUN000001", "recommendation": "Scale UP"}]}
        }
        
        # We write to a temporary file representing output/execution_output.json
        output_file = str(tmp_path / "mock_execution_output.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(mock_output, f)
            
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir}):
            # Run Ingestion 1: First Insertion
            stats1 = ingest_real_operational_data(collection_name="operational_memory_test", execution_output_path=output_file)
            assert stats1["qualifying_incidents"] == 1
            assert stats1["inserted"] == 1
            assert stats1["duplicates_skipped"] == 0
            
            # Run Ingestion 2: Idempotency check (Duplicate skipped)
            stats2 = ingest_real_operational_data(collection_name="operational_memory_test", execution_output_path=output_file)
            assert stats2["qualifying_incidents"] == 1
            assert stats2["inserted"] == 0
            assert stats2["duplicates_skipped"] == 1
            
            # Verify record contains traceability fields in retrieved dictionary
            service = MemoryService(collection_name="operational_memory_test")
            # Let's search by query instead to find the record ID
            results = service.search_similar_memories("Airflow pipeline", limit=1)
            assert len(results) == 1
            metadata = results[0]["metadata"]
            assert metadata["source_dataset"] == "airflow"
            assert metadata["source_record_id"] == "RUN000001"
            assert metadata["memory_version"] == "2.0"
