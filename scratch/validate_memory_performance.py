import os
import json
import time
import logging
from typing import Dict, Any
from unittest.mock import patch, MagicMock

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Import agents and retriever
from agents.behavior.behavior_agent import BehaviorAgent
from agents.risk.risk_agent import RiskPredictionAgent
from services.memory_retriever import OperationalMemoryRetriever
from services.memory_service import MemoryService, _embedding_cache, _cache_lock

def run_aif_pipeline(observation: Dict[str, Any], memory_enabled: bool, cache_enabled: bool = True) -> Dict[str, Any]:
    # Set config environment variables
    os.environ["OPERATIONAL_MEMORY_ENABLED"] = "true" if memory_enabled else "false"
    os.environ["OPERATIONAL_MEMORY_EMBED_CACHE_ENABLED"] = "true" if cache_enabled else "false"
    
    start_time = time.perf_counter()
    
    # 1. Behavior Agent (includes retrieval if enabled)
    behavior_agent = BehaviorAgent("config/behavior_rules.yaml")
    behavior_obj = behavior_agent.analyze_observation(observation)
    
    # 2. Risk Agent
    risk_agent = RiskPredictionAgent("config/risk_rules.yaml")
    risk_obj = risk_agent.predict_risk(behavior_obj)
    
    total_latency = (time.perf_counter() - start_time) * 1000  # ms
    
    # Extract retrieval specific sub-metrics if enabled
    retrieval_stats = {}
    if memory_enabled:
        hist = behavior_obj.get("historical_context", {})
        retrieval_stats = {
            "query": hist.get("retrieval_query", ""),
            "count": hist.get("retrieval_count", 0),
            "available": hist.get("available", False),
            "matches": hist.get("matches", [])
        }
        
    return {
        "behavior": behavior_obj,
        "risk": risk_obj,
        "total_latency": total_latency,
        "retrieval": retrieval_stats
    }

def main():
    print("=" * 80)
    print("PHASE 3.1: OPERATIONAL RETRIEVAL PERFORMANCE VALIDATION")
    print("=" * 80)
    
    registry_path = "output/execution_output.json"
    if not os.path.exists(registry_path):
        print("Error: Registry file output/execution_output.json not found.")
        return
        
    with open(registry_path, "r", encoding="utf-8") as f:
        registry = json.load(f)
        
    observer_output = registry.get("observer_output", {})
    
    # Select test cases: Kafka BRO0001 and Manufacturing MAC0008
    test_cases = []
    
    # Kafka
    for r in observer_output.get("kafka", []):
        if r.get("entity_id") == "BRO0001":
            test_cases.append(("KAFKA BRO0001", r))
            break
            
    # Manufacturing
    for r in observer_output.get("manufacturing", []):
        if r.get("entity_id") == "MAC0008":
            test_cases.append(("MANUFACTURING MAC0008", r))
            break
            
    if not test_cases:
        print("Required test cases not found.")
        return
        
    # Standard query search evaluation for Phase 2.1 semantic quality check
    eval_queries = [
        "Kafka consumer lag topic partition delay",
        "Airflow pipeline execution failed task error",
        "Manufacturing machine welding defect count anomaly"
    ]
    
    for sys_label, obs in test_cases:
        print(f"\nPROFILING FOR {sys_label}:")
        print("=" * 60)
        
        # Clear Cache first
        with _cache_lock:
            _embedding_cache.clear()
            
        # ----------------------------------------------------
        # MEASUREMENT A: MEMORY OFF
        # ----------------------------------------------------
        res_a = run_aif_pipeline(obs, memory_enabled=False)
        
        # ----------------------------------------------------
        # MEASUREMENT B: MEMORY ON, UNOPTIMIZED (Simulated)
        # ----------------------------------------------------
        # We patch requests.Session.post back to standard requests.post to simulate connection recreation,
        # and disable embedding cache.
        import requests
        with patch("services.memory_service._http_session.post", side_effect=requests.post) as mock_post:
            res_b = run_aif_pipeline(obs, memory_enabled=True, cache_enabled=False)
            # Count Ollama calls (must be 1 for retrieval)
            ollama_calls_b = mock_post.call_count
            
        # ----------------------------------------------------
        # MEASUREMENT C: MEMORY ON, OPTIMIZED (Cache Miss & Session Reuse)
        # ----------------------------------------------------
        # Clear Cache to force miss but use pooled session
        with _cache_lock:
            _embedding_cache.clear()
            
        start_c_miss = time.perf_counter()
        res_c_miss = run_aif_pipeline(obs, memory_enabled=True, cache_enabled=True)
        latency_c_miss = (time.perf_counter() - start_c_miss) * 1000
        
        # ----------------------------------------------------
        # MEASUREMENT D: MEMORY ON, OPTIMIZED (Cache Hit)
        # ----------------------------------------------------
        # Keep Cache populated, run a second time to hit cache
        start_c_hit = time.perf_counter()
        res_c_hit = run_aif_pipeline(obs, memory_enabled=True, cache_enabled=True)
        latency_c_hit = (time.perf_counter() - start_c_hit) * 1000
        
        # Verify Risk Invariance
        score_a = res_a["risk"]["risk_score"]
        score_b = res_b["risk"]["risk_score"]
        score_c = res_c_miss["risk"]["risk_score"]
        
        sev_a = res_a["risk"]["risk_severity"]
        sev_b = res_b["risk"]["risk_severity"]
        sev_c = res_c_miss["risk"]["risk_severity"]
        
        conf_a = res_a["risk"]["prediction_confidence"]
        conf_b = res_b["risk"]["prediction_confidence"]
        conf_c = res_c_miss["risk"]["prediction_confidence"]
        
        invariance_ok = (score_a == score_b == score_c) and (sev_a == sev_b == sev_c) and (conf_a == conf_b == conf_c)
        invariance_str = "PASS" if invariance_ok else "FAIL"
        
        print(f"Risk Score Invariance   : {invariance_str}")
        print(f"  Risk Scores (OFF / ON-Unopt / ON-Opt) : {score_a} / {score_b} / {score_c}")
        print(f"  Severities  (OFF / ON-Unopt / ON-Opt) : {sev_a} / {sev_b} / {sev_c}")
        print(f"  Confidences (OFF / ON-Unopt / ON-Opt) : {conf_a:.2f} / {conf_b:.2f} / {conf_c:.2f}")
        print("-" * 60)
        
        # Latency Comparison
        latency_a = res_a["total_latency"]
        latency_b = res_b["total_latency"]
        
        abs_imp_miss = latency_b - latency_c_miss
        pct_imp_miss = (abs_imp_miss / latency_b) * 100
        
        abs_imp_hit = latency_b - latency_c_hit
        pct_imp_hit = (abs_imp_hit / latency_b) * 100
        
        print("Latency Profiling Summary:")
        print(f"  A. Baseline (Memory OFF)              : {latency_a:.2f} ms")
        print(f"  B. Memory ON (Unoptimized)            : {latency_b:.2f} ms")
        print(f"  C. Memory ON (Optimized - Cache MISS) : {latency_c_miss:.2f} ms")
        print(f"  D. Memory ON (Optimized - Cache HIT)  : {latency_c_hit:.2f} ms")
        print("-" * 60)
        
        print("Performance Improvement (MISS vs. Unoptimized):")
        print(f"  Absolute Improvement                  : {abs_imp_miss:.2f} ms")
        print(f"  Percentage Improvement                : {pct_imp_miss:.2f}%")
        print("Performance Improvement (HIT vs. Unoptimized):")
        print(f"  Absolute Improvement                  : {abs_imp_hit:.2f} ms")
        print(f"  Percentage Improvement                : {pct_imp_hit:.2f}%")
        print("-" * 60)
        
        print("Retrieval Match Diagnostics:")
        hist_b = res_b["retrieval"]
        hist_c = res_c_miss["retrieval"]
        print(f"  Historical Context Available          : {hist_c.get('available')}")
        print(f"  Retrieval Query                       : '{hist_c.get('query')[:60]}...'")
        print(f"  Matches (Unoptimized vs. Optimized)  : {hist_b.get('count')} vs. {hist_c.get('count')}")
        for idx, m in enumerate(hist_c.get("matches", [])):
            print(f"    Match #{idx+1}: {m['incident_type']} (ID: {m['incident_id']}, Distance: {m['distance']:.4f})")
        print("=" * 60)
        
    # ----------------------------------------------------
    # RETRIEVAL QUALITY PRESERVATION TEST
    # ----------------------------------------------------
    print("\nRETRIEVAL QUALITY PRESERVATION CHECKS:")
    print("=" * 60)
    service = MemoryService(collection_name="operational_memory")
    if service.is_available:
        for q in eval_queries:
            # Query once to populate cache
            res_opt_1 = service.search_similar_memories(q, limit=3)
            
            # Query again (must be cache HIT)
            start_hit = time.perf_counter()
            res_opt_2 = service.search_similar_memories(q, limit=3)
            hit_lat = (time.perf_counter() - start_hit) * 1000
            
            # Verify exact matches quality preservation
            matches_match = len(res_opt_1) == len(res_opt_2) and all(
                res_opt_1[i]["memory_id"] == res_opt_2[i]["memory_id"] for i in range(len(res_opt_1))
            )
            quality_str = "PRESERVED" if matches_match else "DEGRADED"
            print(f"Query: '{q}'")
            print(f"  Semantic Match Consistency : {quality_str}")
            print(f"  Cache Hit Retrieval Latency : {hit_lat:.2f} ms")
            if res_opt_2:
                print(f"  Top Match ID: {res_opt_2[0]['memory_id']} (Distance: {res_opt_2[0]['distance']:.4f})")
            print("-" * 50)
            
if __name__ == "__main__":
    main()
