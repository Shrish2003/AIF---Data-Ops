import os
import pytest
import threading
from unittest.mock import patch, MagicMock
from collections import OrderedDict
from services.memory_service import (
    MemoryService,
    _embedding_cache,
    _cache_lock,
    is_cache_enabled,
    get_cache_size_limit,
    _http_session
)

@pytest.fixture(autouse=True)
def setup_cache():
    # Clear cache before and after every test
    with _cache_lock:
        _embedding_cache.clear()
    yield
    with _cache_lock:
        _embedding_cache.clear()

def test_http_session_reuse():
    import requests as req_lib
    # Verify the global session is a proper requests.Session (connection pooling enabled)
    assert _http_session is not None
    assert isinstance(_http_session, req_lib.Session)

def test_cache_hit_and_miss():
    mock_vector = [0.1, 0.2, 0.3]
    service = MemoryService()
    
    with patch("services.memory_service.is_cache_enabled", return_value=True), \
         patch("services.memory_service._http_session.post") as mock_post:
        
        # Mock Ollama HTTP response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_vector}
        mock_post.return_value = mock_response
        
        # Miss 1: Not in cache
        vec_1 = service._generate_embedding("query one")
        assert vec_1 == mock_vector
        assert mock_post.call_count == 1
        
        # Hit 2: Already in cache
        vec_2 = service._generate_embedding("query one")
        assert vec_2 == mock_vector
        assert mock_post.call_count == 1  # call count does not increase!

def test_exact_query_matching():
    mock_vector = [0.1, 0.2, 0.3]
    service = MemoryService()
    
    with patch("services.memory_service.is_cache_enabled", return_value=True), \
         patch("services.memory_service._http_session.post") as mock_post:
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_vector}
        mock_post.return_value = mock_response
        
        service._generate_embedding("query one")
        assert mock_post.call_count == 1
        
        # Different query must miss cache
        service._generate_embedding("query two")
        assert mock_post.call_count == 2

def test_lru_eviction_and_size_limit():
    service = MemoryService()
    mock_vector = [0.1]
    
    with patch("services.memory_service.is_cache_enabled", return_value=True), \
         patch("services.memory_service.get_cache_size_limit", return_value=3), \
         patch("services.memory_service._http_session.post") as mock_post:
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_vector}
        mock_post.return_value = mock_response
        
        # Insert 3 entries
        service._generate_embedding("q1")
        service._generate_embedding("q2")
        service._generate_embedding("q3")
        
        with _cache_lock:
            assert len(_embedding_cache) == 3
            assert "q1" in _embedding_cache
            
        # Insert 4th entry, evicting oldest ("q1")
        service._generate_embedding("q4")
        
        with _cache_lock:
            assert len(_embedding_cache) == 3
            assert "q1" not in _embedding_cache
            assert "q4" in _embedding_cache

def test_cache_disabled():
    service = MemoryService()
    mock_vector = [0.1]
    
    with patch("services.memory_service.is_cache_enabled", return_value=False), \
         patch("services.memory_service._http_session.post") as mock_post:
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_vector}
        mock_post.return_value = mock_response
        
        # Miss 1
        service._generate_embedding("q1")
        # Miss 2 (since cache is disabled, it must call Ollama again)
        service._generate_embedding("q1")
        
        assert mock_post.call_count == 2
        with _cache_lock:
            assert len(_embedding_cache) == 0

def test_concurrent_cache_access():
    # Test that concurrent threads can read/write cache without deadlock or unbounded growth
    service = MemoryService()
    mock_vector = [0.1]
    
    with patch("services.memory_service.is_cache_enabled", return_value=True), \
         patch("services.memory_service.get_cache_size_limit", return_value=10), \
         patch("services.memory_service._http_session.post") as mock_post:
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"embedding": mock_vector}
        mock_post.return_value = mock_response
        
        def worker(num):
            service._generate_embedding(f"thread_query_{num}")
            
        threads = [threading.Thread(target=worker, args=(i % 5,)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
            
        with _cache_lock:
            # We had 5 unique queries, so size must be exactly 5
            assert len(_embedding_cache) == 5

def test_warmup_success_and_failure():
    service = MemoryService()
    
    with patch("services.memory_service.MemoryService._generate_embedding") as mock_gen:
        # Success case
        service._warmup_model()
        assert mock_gen.call_count == 1
        
        # Failure case (non-fatal, should log a warning but not crash)
        mock_gen.side_effect = Exception("Ollama offline")
        service._warmup_model()  # should not throw exception
