import os
import sys
sys.path.insert(0, os.path.abspath("."))
import json
import time
from typing import Dict, Any
from unittest.mock import patch

# Load environment
from dotenv import load_dotenv
load_dotenv()

from agents.behavior.behavior_agent import BehaviorAgent
from agents.risk.risk_agent import RiskPredictionAgent
from agents.integrity.integrity_agent import IntegrityAgent
from agents.recommendation.recommendation_agent import RecommendationAgent
from api.services.context_builder import OperationalIntelligenceContextBuilder
from api.services.copilot_service import CopilotService
from services.phase4_orchestrator import compute_phase4_intelligence
from services.historical_intelligence import HistoricalIntelligenceService
from services.root_cause_service import RootCauseService
from services.dependency_intelligence import DependencyIntelligenceService
from services.business_impact_service import BusinessImpactService
from services.recommendation_evidence import RecommendationEvidenceService

def find_observation(registry: dict, entity_id: str):
    obs_dict = registry.get("observer_output", {})
    for system_name, obs_list in obs_dict.items():
        for obs in obs_list:
            if obs.get("entity_id") == entity_id:
                return system_name, obs
    return None, None

def find_full_entity(registry: dict, entity_id: str):
    op_entities = registry.get("operational_entity", {})
    for system_name, ent_list in op_entities.items():
        for ent in ent_list:
            if ent.get("entity_id") == entity_id:
                return system_name, ent
    return None, None

def run_e2e_for_entity(entity_id: str, registry: dict):
    print(f"\n=======================================================")
    print(f"RUNNING COMPLETE E2E FLOW FOR: {entity_id}")
    print(f"=======================================================")
    
    sys_name, obs = find_observation(registry, entity_id)
    if not obs:
        print(f"ERROR: Could not find observation for {entity_id}")
        return None
        
    _, op_ent = find_full_entity(registry, entity_id)
    
    # 1. Raw / Enriched Telemetry
    telemetry = obs.get("observations", {}).get("metrics", {})
    events = obs.get("observations", {}).get("events", [])
    print(f"1. Telemetry Metrics: {telemetry}")
    print(f"   Events: {events}")
    
    # 2. Observer Output
    print(f"2. Observer Output: events={events}, baseline={obs.get('observations', {}).get('baseline', {})}")
    
    # 3. Behavior Output (with memory ON)
    os.environ["OPERATIONAL_MEMORY_ENABLED"] = "true"
    behavior_agent = BehaviorAgent("config/behavior_rules.yaml")
    
    # Run behavior
    t0 = time.perf_counter()
    beh_obj = behavior_agent.analyze_observation(obs)
    beh_time_ms = (time.perf_counter() - t0) * 1000
    beh_data = beh_obj.get("behavior", {})
    print(f"3. Behavior Output: score={beh_data.get('behavior_score')}, severity={beh_data.get('severity')}, patterns={beh_data.get('patterns')}")
    
    # 4. Historical Memory Matches
    hist_context = beh_obj.get("historical_context", {})
    matches = hist_context.get("matches", [])
    print(f"4. Historical Context: available={hist_context.get('available')}, count={hist_context.get('retrieval_count')}")
    for i, m in enumerate(matches[:3]):
        print(f"   Match {i+1}: id={m.get('incident_id')}, type={m.get('incident_type')}, dist={m.get('distance')}, cause={m.get('root_cause')}")
        
    # Phase 4 Intelligence
    p4_intel = beh_obj.get("phase4_intelligence", {})
    print(f"   Phase 4 available: {p4_intel.get('available')}")
    
    # 5. Historical Intelligence
    hist_intel = p4_intel.get("historical_intelligence", {})
    print(f"5. Historical Intelligence: available={hist_intel.get('available')}, matches={hist_intel.get('match_count')}")
    print(f"   Recurring root causes: {hist_intel.get('recurring_root_causes')}")
    print(f"   Historical outcomes: {hist_intel.get('historical_outcomes')}")
    print(f"   Summary: {hist_intel.get('summary')}")
    
    # 6. RCA Hypotheses
    rca = p4_intel.get("root_cause_intelligence", {})
    print(f"6. RCA Hypotheses: available={rca.get('available')}")
    for h in rca.get("primary_hypotheses", []):
        print(f"   • Hypothesis: {h.get('cause')}")
        print(f"     Confidence: {h.get('confidence')} ({h.get('confidence_level')}), Source: {h.get('source')}")
        print(f"     Evidence: {h.get('evidence')}")
        
    # 7. Dependency Intelligence
    dep = p4_intel.get("dependency_intelligence", {})
    print(f"7. Dependency Intelligence: available={dep.get('available')}")
    print(f"   Upstream: {dep.get('upstream')}, Downstream: {dep.get('downstream')}")
    print(f"   Dependency Type: {dep.get('dependency_type')}, Depth: {dep.get('impact_depth')}")
    print(f"   Direct Impact: {dep.get('direct_impact')}, Transitive: {dep.get('transitive_impact')}")
    
    # 8. Business Impact
    biz = p4_intel.get("business_impact", {})
    print(f"8. Business Impact: available={biz.get('available')}")
    print(f"   Unit: {biz.get('business_unit')}, App: {biz.get('application')}, Criticality: {biz.get('criticality')}")
    print(f"   Impact Level: {biz.get('impact_level')}, SLA: {biz.get('sla_minutes')}m, Affected: {biz.get('affected_services')}")
    
    # 9. Recommendation Evidence
    rec_ev = p4_intel.get("recommendation_evidence", {})
    print(f"9. Recommendation Evidence: available={rec_ev.get('available')}")
    print(f"   Rec: {rec_ev.get('recommendation')}, Applied: {rec_ev.get('applied_previously')}")
    print(f"   Success Rate: {rec_ev.get('success_rate')}% (used: {rec_ev.get('historical_usage_count')}, success: {rec_ev.get('successful_count')})")
    print(f"   Evidence Summary: {rec_ev.get('evidence_summary')}")
    
    # 10. Risk Output
    risk_agent = RiskPredictionAgent("config/risk_rules.yaml")
    t_risk0 = time.perf_counter()
    risk_obj = risk_agent.predict_risk(beh_obj)
    risk_time_ms = (time.perf_counter() - t_risk0) * 1000
    print(f"10. Risk Output: score={risk_obj.get('risk_score')}, severity={risk_obj.get('risk_severity')}, prob={risk_obj.get('risk_probability')}%, conf={risk_obj.get('prediction_confidence')}")
    print(f"    Risk carries phase4_intelligence: {'phase4_intelligence' in risk_obj}")
    
    # 11. Integrity Output
    integrity_agent = IntegrityAgent("config/integrity_rules.yaml")
    integ_obj = integrity_agent.evaluate_integrity(obs, op_ent)
    print(f"11. Integrity Output: score={integ_obj.get('integrity_score')}, status={integ_obj.get('integrity_status')}, trust={integ_obj.get('output_trust_level')}")
    
    # 12. Recommendation Output
    rec_agent = RecommendationAgent("config/recommendation_rules.yaml")
    rec_obj = rec_agent.generate_recommendation(beh_obj, risk_obj, integ_obj, op_ent)
    print(f"12. Recommendation Output: priority={rec_obj.get('priority')}")
    print(f"    Action: {rec_obj.get('recommendation')}")
    
    # 13. Context Builder Output
    ctx = OperationalIntelligenceContextBuilder.build_context(
        pipeline_id=entity_id,
        use_case_id="data-ops",
        domain_id="default"
    )
    p4_in_ctx = ctx.get("agentIntelligence", {}).get("phase4Intelligence")
    print(f"13. Final Context Builder: phase4Intelligence present? {p4_in_ctx is not None}")
    if p4_in_ctx:
        print(f"    Context Phase 4 available: {p4_in_ctx.get('available')}")
        print(f"    Context RCA count: {len(p4_in_ctx.get('root_cause_intelligence', {}).get('primary_hypotheses', []))}")
        print(f"    Context Business Unit: {p4_in_ctx.get('business_impact', {}).get('business_unit')}")
        print(f"    Context Dep Downstream: {p4_in_ctx.get('dependency_intelligence', {}).get('downstream')}")
        
    return {
        "telemetry": telemetry,
        "obs": obs,
        "beh": beh_obj,
        "hist_matches": matches,
        "hist_intel": hist_intel,
        "rca": rca,
        "dep": dep,
        "biz": biz,
        "rec_ev": rec_ev,
        "risk": risk_obj,
        "integ": integ_obj,
        "rec": rec_obj,
        "context": ctx,
        "beh_time_ms": beh_time_ms,
        "risk_time_ms": risk_time_ms
    }

def test_risk_invariance(entity_id: str, registry: dict):
    sys_name, obs = find_observation(registry, entity_id)
    behavior_agent = BehaviorAgent("config/behavior_rules.yaml")
    risk_agent = RiskPredictionAgent("config/risk_rules.yaml")
    
    # Memory OFF
    os.environ["OPERATIONAL_MEMORY_ENABLED"] = "false"
    beh_off = behavior_agent.analyze_observation(obs)
    risk_off = risk_agent.predict_risk(beh_off)
    
    # Memory ON
    os.environ["OPERATIONAL_MEMORY_ENABLED"] = "true"
    beh_on = behavior_agent.analyze_observation(obs)
    risk_on = risk_agent.predict_risk(beh_on)
    
    # Compare
    beh_score_equal = (beh_off["behavior"]["behavior_score"] == beh_on["behavior"]["behavior_score"])
    beh_sev_equal = (beh_off["behavior"]["severity"] == beh_on["behavior"]["severity"])
    risk_score_equal = (risk_off["risk_score"] == risk_on["risk_score"])
    risk_sev_equal = (risk_off["risk_severity"] == risk_on["risk_severity"])
    risk_conf_equal = (risk_off["prediction_confidence"] == risk_on["prediction_confidence"])
    risk_prob_equal = (risk_off["risk_probability"] == risk_on["risk_probability"])
    risk_cat_equal = (risk_off["risk_category"] == risk_on["risk_category"])
    
    all_equal = all([beh_score_equal, beh_sev_equal, risk_score_equal, risk_sev_equal, risk_conf_equal, risk_prob_equal, risk_cat_equal])
    print(f"\n--- RISK INVARIANCE FOR {entity_id} ---")
    print(f"Memory OFF: risk_score={risk_off['risk_score']}, severity={risk_off['risk_severity']}, conf={risk_off['prediction_confidence']}, prob={risk_off['risk_probability']}")
    print(f"Memory ON:  risk_score={risk_on['risk_score']}, severity={risk_on['risk_severity']}, conf={risk_on['prediction_confidence']}, prob={risk_on['risk_probability']}")
    print(f"EXACTLY IDENTICAL: {all_equal}")
    return all_equal

def test_external_calls_during_phase4():
    print(f"\n--- PHASE 4 EXTERNAL CALL AUDIT ---")
    # Patch chromadb, requests.post (Ollama), open/csv
    import chromadb
    import requests
    
    chroma_called = [0]
    ollama_called = [0]
    
    # Sample behavior object with memory already populated
    sample_beh = {
        "entity_id": "TEST_001",
        "observation": {
            "entity": {
                "context": {
                    "business_context": {"criticality": "Critical", "business_unit": "Finance"},
                    "incident_context": {"root_cause": "Network issue"},
                    "lineage_context": {"depends_on": ["DAG001"], "downstream": []},
                    "recommendation_context": {"recommendation": "Restart", "success_rate": 90}
                }
            }
        },
        "historical_context": {
            "available": True,
            "matches": [
                {"root_cause": "Network issue", "action_taken": "Restarted", "outcome": "Success", "resolution_status": "RESOLVED"}
            ]
        },
        "behavior": {"severity": "WARNING", "deviation": {}, "patterns": []}
    }
    
    with patch.object(requests, "post", side_effect=Exception("Ollama called!")) as mock_post:
        try:
            p4_out = compute_phase4_intelligence(sample_beh)
            print(f"Phase 4 executed with 0 Ollama calls: SUCCESS (available={p4_out.get('available')})")
        except Exception as e:
            print(f"Phase 4 called Ollama: FAIL ({e})")
            
    # Test ChromaDB
    with patch.object(chromadb, "PersistentClient", side_effect=Exception("ChromaDB called!")):
        try:
            p4_out = compute_phase4_intelligence(sample_beh)
            print(f"Phase 4 executed with 0 ChromaDB calls: SUCCESS")
        except Exception as e:
            print(f"Phase 4 called ChromaDB: FAIL ({e})")

def test_failure_isolation():
    print(f"\n--- PHASE 4 FAILURE ISOLATION AUDIT ---")
    cases = [
        ("1. Empty historical_context", {"observation": {"entity": {"context": {}}}, "historical_context": {}, "behavior": {}}),
        ("2. Missing business_context", {"observation": {"entity": {"context": {"lineage_context": {}}}}, "historical_context": {"matches": []}, "behavior": {}}),
        ("3. Missing lineage_context", {"observation": {"entity": {"context": {"business_context": {}}}}, "historical_context": {"matches": []}, "behavior": {}}),
        ("4. Missing recommendation_context", {"observation": {"entity": {"context": {}}}, "historical_context": {"matches": []}, "behavior": {}}),
        ("5. Missing incident_context", {"observation": {"entity": {"context": {}}}, "historical_context": {"matches": []}, "behavior": {}}),
        ("6. Malformed historical match", {"observation": {"entity": {"context": {}}}, "historical_context": {"matches": [{"malformed": True}, None]}, "behavior": {}}),
    ]
    all_isolated = True
    for name, beh in cases:
        try:
            res = compute_phase4_intelligence(beh)
            print(f"  Passed {name}: isolated successfully (available={res.get('available')})")
        except Exception as e:
            print(f"  FAILED {name}: crashed with {e}")
            all_isolated = False
    print(f"All failure isolation cases passed: {all_isolated}")

def test_performance(entity_id: str, registry: dict):
    print(f"\n--- PERFORMANCE VALIDATION FOR {entity_id} ---")
    sys_name, obs = find_observation(registry, entity_id)
    behavior_agent = BehaviorAgent("config/behavior_rules.yaml")
    
    # Measure Memory OFF
    os.environ["OPERATIONAL_MEMORY_ENABLED"] = "false"
    t0 = time.perf_counter()
    beh_off = behavior_agent.analyze_observation(obs)
    off_latency = (time.perf_counter() - t0) * 1000
    
    # Measure Memory ON
    os.environ["OPERATIONAL_MEMORY_ENABLED"] = "true"
    t1 = time.perf_counter()
    beh_on = behavior_agent.analyze_observation(obs)
    on_latency = (time.perf_counter() - t1) * 1000
    
    # Measure sub-services isolated
    entity_context = beh_on.get("observation", {}).get("entity", {}).get("context", {})
    hist_ctx = beh_on.get("historical_context", {})
    
    t_hist = time.perf_counter()
    hist_intel = HistoricalIntelligenceService.analyze(hist_ctx)
    t_hist_ms = (time.perf_counter() - t_hist) * 1000
    
    t_rca = time.perf_counter()
    rca = RootCauseService.analyze(beh_on, hist_intel, entity_context.get("incident_context", {}))
    t_rca_ms = (time.perf_counter() - t_rca) * 1000
    
    t_dep = time.perf_counter()
    dep = DependencyIntelligenceService.analyze(entity_context.get("lineage_context", {}), entity_id)
    t_dep_ms = (time.perf_counter() - t_dep) * 1000
    
    t_biz = time.perf_counter()
    biz = BusinessImpactService.analyze(entity_context.get("business_context", {}), beh_on)
    t_biz_ms = (time.perf_counter() - t_biz) * 1000
    
    t_rec = time.perf_counter()
    rec_ev = RecommendationEvidenceService.analyze(entity_context.get("recommendation_context", {}), entity_context.get("incident_context", {}), hist_ctx)
    t_rec_ms = (time.perf_counter() - t_rec) * 1000
    
    total_p4_ms = t_hist_ms + t_rca_ms + t_dep_ms + t_biz_ms + t_rec_ms
    
    print(f"Memory OFF total latency : {off_latency:.2f} ms")
    print(f"Memory ON total latency  : {on_latency:.2f} ms")
    print(f"Phase 4 total latency    : {total_p4_ms:.3f} ms")
    print(f"  - Historical Intel     : {t_hist_ms:.3f} ms")
    print(f"  - RCA Service          : {t_rca_ms:.3f} ms")
    print(f"  - Dependency Intel     : {t_dep_ms:.3f} ms")
    print(f"  - Business Impact      : {t_biz_ms:.3f} ms")
    print(f"  - Rec Evidence         : {t_rec_ms:.3f} ms")

def test_copilot_questions():
    print(f"\n--- COPILOT REAL QUERY TEST (BRO0001) ---")
    from api.schemas.request import CopilotQuery
    
    questions = [
        "What happened with BRO0001?",
        "Have we seen similar incidents before?",
        "What are the likely root causes?",
        "What services could be affected?",
        "What is the business impact?",
        "What recommendation worked historically?",
        "Why are you recommending this action?",
        "What evidence supports your conclusion?"
    ]
    
    # Pre-build context for BRO0001
    ctx = OperationalIntelligenceContextBuilder.build_context("BRO0001", "data-ops", "default")
    
    for i, q in enumerate(questions, 1):
        payload = CopilotQuery(
            useCaseId="data-ops",
            domainId="default",
            question=q,
            context=ctx
        )
        t0 = time.perf_counter()
        resp = CopilotService.chat(payload)
        dur = (time.perf_counter() - t0) * 1000
        reply = resp.get("reply", "")
        # Truncate reply to 150 chars for summary
        short_reply = reply[:150].replace("\n", " ")
        print(f"Q{i}: \"{q}\"")
        print(f"   Status: {resp.get('status')}, Conf: {resp.get('confidence')}, Duration: {dur:.1f}ms")
        print(f"   Root Cause: {resp.get('rootCause')}")
        print(f"   Business Impact: {resp.get('businessImpact')}")
        print(f"   Reply excerpt: {short_reply}...")

def main():
    registry_path = "output/execution_output.json"
    if not os.path.exists(registry_path):
        print(f"ERROR: {registry_path} not found.")
        return
    with open(registry_path, "r", encoding="utf-8") as f:
        registry = json.load(f)
        
    # E2E Test BRO0001 & MAC0008
    res_bro = run_e2e_for_entity("BRO0001", registry)
    res_mac = run_e2e_for_entity("MAC0008", registry)
    
    # Risk Invariance
    test_risk_invariance("BRO0001", registry)
    test_risk_invariance("MAC0008", registry)
    
    # External call audit (0 ChromaDB, 0 Ollama)
    test_external_calls_during_phase4()
    
    # Failure Isolation
    test_failure_isolation()
    
    # Performance
    test_performance("BRO0001", registry)
    test_performance("MAC0008", registry)
    
    # Copilot Questions
    test_copilot_questions()

if __name__ == "__main__":
    main()
