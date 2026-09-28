import os
import json
import pytest
from unittest.mock import patch, MagicMock
import requests
from services.memory_service import MemoryService, create_semantic_document, OperationalMemoryRecord

# ==========================================================
# Test Fixtures
# ==========================================================
@pytest.fixture
def mock_embedding():
    return [0.05] * 128

@pytest.fixture
def sample_incident():
    return {
        "incident_id": "INC_TEST_001",
        "pipeline_id": "BRO0001",
        "timestamp": "2026-08-22T12:00:00Z",
        "incident_type": "Kafka Consumer Lag",
        "summary": "Kafka lag is high on BRO0001 downstream topic",
        "symptoms": "Consumer group transaction-consumer lag exceeded 100k messages",
        "behavior": "Throughput dropped by 45%",
        "root_cause": "Kafka consumer crashed during partition rebalance",
        "risk": "HIGH",
        "affected_services": ["payment-service", "analytics-dashboard"],
        "business_impact": "Downstream transactions delayed",
        "recommendation": "Restart Kafka consumer and trigger partition replay",
        "action_taken": "Restarted pods and validated partition assignment",
        "outcome": "Pipeline fully recovered and caught up on lag",
        "resolution_status": "RESOLVED"
    }

@pytest.fixture
def temp_db_dir(tmp_path):
    return str(tmp_path / "chromadb_test")


# ==========================================================
# Unit Tests
# ==========================================================

class TestOperationalMemory:

    # 1. Test basic ChromaDB initialization and initialization failure handling
    @patch('requests.get')
    @patch('chromadb.PersistentClient')
    def test_chromadb_initialization_failure(self, mock_client, mock_get, temp_db_dir):
        # Simulate ChromaDB raising connection exception and Ollama being offline
        mock_client.side_effect = Exception("Failed to bind port or socket")
        mock_get.side_effect = Exception("Ollama offline")
        
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir}):
            service = MemoryService()
            assert service.is_available is False
            assert service._collection is None
            
            # Unavailability check: Operations must fail/return gracefully instead of crashing
            assert service.add_memory({"incident_id": "INC_001"}) is None
            assert service.search_similar_memories("Kafka lag") == []
            assert service.get_memory("mem_abc") is None
            assert service.delete_memory("mem_abc") is False
            
            health = service.health_check()
            assert health["status"] == "unhealthy"
            assert "error: Failed to bind port or socket" in health["chromadb"]["status"]

    # 2. Test memory insertion, validation and list fields JSON serialization
    @patch('requests.post')
    def test_add_memory_validation_and_storage(self, mock_post, temp_db_dir, sample_incident, mock_embedding):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_embedding}
        mock_post.return_value = mock_response
        
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir}):
            service = MemoryService()
            assert service.is_available is True
            
            mem_id = service.add_memory(sample_incident)
            assert mem_id is not None
            assert mem_id.startswith("mem_")
            
            # Fetch back the record and confirm field values
            retrieved = service.get_memory(mem_id)
            assert retrieved is not None
            assert retrieved["memory_id"] == mem_id
            
            metadata = retrieved["metadata"]
            assert metadata["incident_id"] == "INC_TEST_001"
            assert metadata["pipeline_id"] == "BRO0001"
            assert metadata["risk"] == "HIGH"
            
            # Verify list field deserialized correctly from stored string
            assert isinstance(metadata["affected_services"], list)
            assert metadata["affected_services"] == ["payment-service", "analytics-dashboard"]
            assert metadata["resolution_status"] == "RESOLVED"
            assert metadata["root_cause"] == "Kafka consumer crashed during partition rebalance"

    # 3. Test invalid memory schema error propagation
    def test_add_invalid_memory_fields(self, temp_db_dir):
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir}):
            service = MemoryService()
            invalid_incident = {
                "incident_id": "INC_BAD",
                "pipeline_id": "BRO0001",
                # missing required fields (timestamp, incident_type, etc.)
            }
            with pytest.raises(ValueError) as excinfo:
                service.add_memory(invalid_incident)
            assert "Invalid incident schema representation" in str(excinfo.value)

    # 4. Test Ollama embedding API invocation
    @patch('services.memory_service._http_session.post')
    def test_ollama_embedding_endpoint_query(self, mock_post, temp_db_dir, sample_incident, mock_embedding):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_embedding}
        mock_post.return_value = mock_response
        
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir, "OPERATIONAL_MEMORY_EMBED_CACHE_ENABLED": "false"}):
            service = MemoryService()
            mem_id = service.add_memory(sample_incident)
            assert mem_id is not None
            
            # Verify primary endpoint /api/embeddings was called via session
            mock_post.assert_called_with(
                "http://localhost:11434/api/embeddings",
                json={
                    "model": "nomic-embed-text",
                    "prompt": create_semantic_document(sample_incident)
                },
                timeout=30
            )

    # 5. Test semantic search matching & similarity formatting
    @patch('requests.post')
    def test_semantic_search_matching(self, mock_post, temp_db_dir, sample_incident, mock_embedding):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_embedding}
        mock_post.return_value = mock_response
        
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir}):
            service = MemoryService()
            mem_id = service.add_memory(sample_incident)
            
            # Query the database
            results = service.search_similar_memories("Kafka Consumer Lag on pipeline BRO0001", limit=1)
            assert len(results) == 1
            assert results[0]["memory_id"] == mem_id
            assert results[0]["metadata"]["incident_id"] == "INC_TEST_001"
            assert "distance" in results[0]
            assert "document" in results[0]
            assert results[0]["metadata"]["affected_services"] == ["payment-service", "analytics-dashboard"]

    # 6. Test edge case: Empty queries or search results
    @patch('requests.post')
    def test_empty_query_and_search_results(self, mock_post, temp_db_dir, mock_embedding):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_embedding}
        mock_post.return_value = mock_response
        
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir}):
            service = MemoryService()
            
            # Empty query returns empty results immediately
            results_empty_query = service.search_similar_memories("")
            assert results_empty_query == []
            
            # Querying empty database returns empty results
            results_empty_db = service.search_similar_memories("Kafka")
            assert results_empty_db == []

    # 7. Test embedding service failure handling
    @patch('services.memory_service._http_session.post')
    def test_ollama_embedding_failure(self, mock_post, temp_db_dir, sample_incident):
        mock_post.side_effect = requests.exceptions.Timeout("Ollama timed out")
        
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir, "OPERATIONAL_MEMORY_EMBED_CACHE_ENABLED": "false"}):
            service = MemoryService()
            with pytest.raises(requests.exceptions.Timeout):
                service.add_memory(sample_incident)

    # 8. Test persistence/restart survival (memories survive application restart)
    @patch('requests.post')
    def test_stored_memories_survive_restart(self, mock_post, temp_db_dir, sample_incident, mock_embedding):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_embedding}
        mock_post.return_value = mock_response
        
        # Scope 1: Add a record to database
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir}):
            service1 = MemoryService()
            assert service1.is_available is True
            mem_id = service1.add_memory(sample_incident)
            assert mem_id is not None
            
            # Delete service instance explicitly to simulate application shut down
            del service1
            
        # Scope 2: Instantiate new service pointing to same directory to simulate restart
        with patch.dict(os.environ, {"CHROMADB_PERSIST_DIR": temp_db_dir}):
            service2 = MemoryService()
            assert service2.is_available is True
            
            # Retrieve record and confirm details match perfectly
            retrieved = service2.get_memory(mem_id)
            assert retrieved is not None
            assert retrieved["memory_id"] == mem_id
            assert retrieved["metadata"]["incident_id"] == "INC_TEST_001"
            assert retrieved["metadata"]["affected_services"] == ["payment-service", "analytics-dashboard"]
            
            # Perform search to confirm vector index was re-loaded and returns the entry
            search_results = service2.search_similar_memories("Kafka delay issue")
            assert len(search_results) > 0
            assert search_results[0]["memory_id"] == mem_id
