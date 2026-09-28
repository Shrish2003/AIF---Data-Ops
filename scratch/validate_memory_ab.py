import os
import json
import time
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Import agents
from agents.behavior.behavior_agent import BehaviorAgent
from agents.risk.risk_agent import RiskPredictionAgent

def run_pipeline(observation: Dict[str, Any], memory_enabled: bool) -> Dict[str, Any]:
    # Set feature flag
    os.environ["OPERATIONAL_MEMORY_ENABLED"] = "true" if memory_enabled else "false"
    
    start_time = time.perf_counter()
    
    # 1. Run Behavior Agent
    behavior_agent = BehaviorAgent("config/behavior_rules.yaml")
    behavior_obj = behavior_agent.analyze_observation(observation)
    
    # 2. Run Risk Agent
    risk_agent = RiskPredictionAgent("config/risk_rules.yaml")
    risk_obj = risk_agent.predict_risk(behavior_obj)
    
    latency = (time.perf_counter() - start_time) * 1000  # ms
    
    return {
        "behavior": behavior_obj,
        "risk": risk_obj,
        "latency": latency
    }

def main():
    print("=" * 80)
    print("A/B REGRESSION VALIDATION: BASELINE vs. MEMORY-AWARE AIF")
    print("=" * 80)
    
    # Load execution output registry database
    registry_path = "output/execution_output.json"
    if not os.path.exists(registry_path):
        print(f"Error: Registry file {registry_path} not found.")
        return
        
    with open(registry_path, "r", encoding="utf-8") as f:
        registry = json.load(f)
        
    observer_output = registry.get("observer_output", {})
    
    # Select one qualified incident observation from each system
    test_cases = []
    
    # We find observations that are qualified anomalies in each system
    behavior_agent = BehaviorAgent("config/behavior_rules.yaml")
    
    systems = ["airflow", "kafka", "kubernetes", "sap", "manufacturing", "iot"]
    for sys in systems:
        records = observer_output.get(sys, [])
        for r in records:
            # Quick check if it qualifies as an anomaly
            temp_beh = behavior_agent.analyze_observation(r)
            if temp_beh.get("behavior", {}).get("severity") in ("WARNING", "CRITICAL"):
                test_cases.append((sys.upper(), r))
                break
                
    if not test_cases:
        print("No qualifying test cases found.")
        return
        
    print(f"Discovered {len(test_cases)} representative test cases across domains.")
    print("-" * 80)
    
    invariance_failures = 0
    
    for sys_name, obs in test_cases:
        entity_id = obs.get("entity_id")
        entity_name = obs.get("entity_name") or "Unknown"
        
        print(f"\nEvaluating System: {sys_name} | Entity: {entity_name} ({entity_id})")
        print("-" * 50)
        
        # Run A: Baseline (Memory OFF)
        res_a = run_pipeline(obs, memory_enabled=False)
        
        # Run B: Memory-Aware (Memory ON)
        res_b = run_pipeline(obs, memory_enabled=True)
        
        # Extract results
        risk_a = res_a["risk"]
        risk_b = res_b["risk"]
        
        beh_a = res_a["behavior"]
        beh_b = res_b["behavior"]
        
        score_a = risk_a["risk_score"]
        score_b = risk_b["risk_score"]
        
        sev_a = risk_a["risk_severity"]
        sev_b = risk_b["risk_severity"]
        
        conf_a = risk_a["prediction_confidence"]
        conf_b = risk_b["prediction_confidence"]
        
        hist_b = beh_b.get("historical_context", {})
        available_b = hist_b.get("available", False)
        matches_b = hist_b.get("matches", [])
        
        # 1. Compare numerical risk score invariance
        is_invariant = (score_a == score_b) and (sev_a == sev_b) and (conf_a == conf_b)
        invariance_str = "PASS (INVARIANT)" if is_invariant else "FAIL (VARIANT)"
        if not is_invariant:
            invariance_failures += 1
            
        print(f"Risk Score Invariance : {invariance_str}")
        print(f"  Baseline (Memory OFF)   -> Score: {score_a}, Severity: {sev_a}, Confidence: {conf_a:.2f}")
        print(f"  Memory-Aware (Memory ON)-> Score: {score_b}, Severity: {sev_b}, Confidence: {conf_b:.2f}")
        
        print(f"Historical Context In B  :")
        print(f"  Available               : {available_b}")
        print(f"  Query                   : '{hist_b.get('retrieval_query')}'")
        print(f"  Matches Found           : {len(matches_b)}")
        
        for rank, m in enumerate(matches_b):
            print(f"    Match #{rank+1}: {m['incident_type']} (ID: {m['incident_id']}, System: {m['source_system']}, Distance: {m['distance']:.4f})")
            print(f"      Root Cause : {m.get('root_cause')}")
            print(f"      Resolution : {m.get('recommendation')}")
            
        print(f"Latency Performance     :")
        print(f"  Baseline (Memory OFF)   : {res_a['latency']:.2f} ms")
        print(f"  Memory-Aware (Memory ON): {res_b['latency']:.2f} ms")
        print(f"  Memory Overhead         : {res_b['latency'] - res_a['latency']:.2f} ms")
        print("-" * 50)
        
    print("\n" + "=" * 80)
    print("A/B REGRESSION VALIDATION COMPLETE")
    print("=" * 80)
    print(f"Total Test Cases   : {len(test_cases)}")
    print(f"Invariance Passes  : {len(test_cases) - invariance_failures}")
    print(f"Invariance Failures: {invariance_failures}")
    print("=" * 80)

if __name__ == "__main__":
    main()
