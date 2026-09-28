import os
import json
import hashlib
import re
from collections import Counter
from datetime import datetime
from typing import Dict, Any, List, Tuple

from services.memory_service import MemoryService
from services.memory_ingestion import OperationalIncidentExtractor, get_lookup_context

def main():
    path = "output/execution_output.json"
    if not os.path.exists(path):
        print(f"Error: {path} not found.")
        return
        
    print("Loading execution output...")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    adapter_output = data.get("adapter_output", {})
    parsed_lookup = adapter_output.get("parsed_lookup", {})
    parsed_raw = adapter_output.get("parsed_raw", {})
    
    op_ent = data.get("operational_entity", {})
    obs_out = data.get("observer_output", {})
    beh_out = data.get("behavior_output", {})
    risk_out = data.get("risk_output", {})
    integ_out = data.get("integrity_output", {})
    
    extractor = OperationalIncidentExtractor(parsed_lookup)
    
    # Qualification counters
    total_scanned = 0
    qualified_count = 0
    
    triggers = {
        "execution_status_failure": 0,
        "behavior_severity_anomaly": 0,
        "risk_severity_trigger": 0,
        "integrity_failure": 0,
        "observer_events": 0,
        "observer_baseline_deviation": 0
    }
    
    detailed_reasons = {
        "execution_failures": 0,
        "cancelled_executions": 0,
        "behavior_warnings": 0,
        "behavior_critical": 0,
        "high_risk": 0,
        "critical_risk": 0,
        "integrity_failures": 0,
        "observer_anomalies": 0
    }
    
    overlap_counts = {
        "exactly_1": 0,
        "exactly_2": 0,
        "3_or_more": 0
    }
    
    # ID collision test collections
    all_memory_ids = []
    all_incident_ids = []
    
    for ds_name, entities in op_ent.items():
        total_scanned += len(entities)
        
        obs_map = {x["entity_id"]: x for x in obs_out.get(ds_name, []) if "entity_id" in x}
        beh_map = {x["entity_id"]: x for x in beh_out.get(ds_name, []) if "entity_id" in x}
        risk_map = {x["entity_id"]: x for x in risk_out.get(ds_name, []) if "entity_id" in x}
        integ_map = {x["entity_id"]: x for x in integ_out.get(ds_name, []) if "entity_id" in x}
        
        for entity in entities:
            eid = entity.get("entity_id")
            if not eid:
                continue
                
            obs_obj = obs_map.get(eid, {})
            beh_obj = beh_map.get(eid, {})
            risk_obj = risk_map.get(eid, {})
            integ_obj = integ_map.get(eid, {})
            
            # Count triggers
            status = entity.get("execution_status")
            beh_sev = beh_obj.get("behavior", {}).get("severity")
            risk_sev = risk_obj.get("risk_severity")
            risk_score = risk_obj.get("risk_score", 0.0)
            integ_status = integ_obj.get("integrity_status")
            obs_events = obs_obj.get("observations", {}).get("events", [])
            obs_baseline = obs_obj.get("observations", {}).get("baseline", {})
            
            active_rules = []
            
            # Check execution failures
            if status == "FAILED":
                detailed_reasons["execution_failures"] += 1
                active_rules.append("execution_status_failure")
            elif status == "CANCELLED":
                detailed_reasons["cancelled_executions"] += 1
                active_rules.append("execution_status_failure")
                
            # Behavior
            if beh_sev == "WARNING":
                detailed_reasons["behavior_warnings"] += 1
                active_rules.append("behavior_severity_anomaly")
            elif beh_sev == "CRITICAL":
                detailed_reasons["behavior_critical"] += 1
                active_rules.append("behavior_severity_anomaly")
                
            # Risk
            if risk_sev == "HIGH" or risk_score >= 70.0:
                detailed_reasons["high_risk"] += 1
                active_rules.append("risk_severity_trigger")
            elif risk_sev == "CRITICAL":
                detailed_reasons["critical_risk"] += 1
                active_rules.append("risk_severity_trigger")
                
            # Integrity
            if integ_status == "FAILED":
                detailed_reasons["integrity_failures"] += 1
                active_rules.append("integrity_failure")
                
            # Observer
            has_obs_deviation = False
            if obs_events:
                detailed_reasons["observer_anomalies"] += 1
                active_rules.append("observer_events")
                has_obs_deviation = True
            else:
                for metric, details in obs_baseline.items():
                    if isinstance(details, dict) and details.get("status") in ("WARNING", "CRITICAL"):
                        detailed_reasons["observer_anomalies"] += 1
                        active_rules.append("observer_baseline_deviation")
                        has_obs_deviation = True
                        break
            
            unique_rules = set(active_rules)
            if unique_rules:
                qualified_count += 1
                for r in unique_rules:
                    triggers[r] += 1
                    
                num_rules = len(unique_rules)
                if num_rules == 1:
                    overlap_counts["exactly_1"] += 1
                elif num_rules == 2:
                    overlap_counts["exactly_2"] += 1
                else:
                    overlap_counts["3_or_more"] += 1
                    
            # Compute IDs to check collisions
            source_system = entity.get("source_system", "Unknown")
            event_timestamp = entity.get("event_timestamp", datetime.now().isoformat())
            
            # Map incident type
            trigger_type = active_rules[0] if active_rules else "none"
            if trigger_type == "execution_status_failure":
                incident_type = f"{entity.get('entity_type')} Execution Failure"
            elif trigger_type == "behavior_severity_anomaly":
                incident_type = "Behavioral Anomaly"
            elif trigger_type == "risk_severity_trigger":
                incident_type = "High Risk Prediction"
            elif trigger_type == "integrity_failure":
                incident_type = "Data Integrity Violation"
            else:
                incident_type = "Operational Incident"
                
            attributes = entity.get("attributes", {})
            raw_run_id = attributes.get("run_id") or attributes.get("pipelineRunId") or attributes.get("job_number")
            if raw_run_id:
                incident_id = str(raw_run_id)
            else:
                incident_id = f"{source_system}_{eid}_{event_timestamp}_{incident_type}"
                
            unique_key = f"{source_system}_{incident_id}"
            hashed = hashlib.md5(unique_key.encode()).hexdigest()[:8]
            memory_id = f"mem_{hashed}"
            
            all_memory_ids.append(memory_id)
            all_incident_ids.append(incident_id)
            
    print("\n" + "="*50)
    print("A. INCIDENT QUALIFICATION RESULTS")
    print("="*50)
    print(f"Total scanned: {total_scanned}")
    print(f"Qualifying: {qualified_count}")
    print(f"Execution failures: {detailed_reasons['execution_failures']}")
    print(f"Cancelled executions: {detailed_reasons['cancelled_executions']}")
    print(f"Behavior warnings: {detailed_reasons['behavior_warnings']}")
    print(f"Behavior critical: {detailed_reasons['behavior_critical']}")
    print(f"High risk: {detailed_reasons['high_risk']}")
    print(f"Critical risk: {detailed_reasons['critical_risk']}")
    print(f"Integrity failures: {detailed_reasons['integrity_failures']}")
    print(f"Observer anomalies: {detailed_reasons['observer_anomalies']}")
    print("\nRule Triggers:")
    for r, count in triggers.items():
        print(f"  - {r}: {count}")
    print("\nOverlap counts:")
    print(f"  - Exactly 1 rule triggered: {overlap_counts['exactly_1']}")
    print(f"  - Exactly 2 rules triggered: {overlap_counts['exactly_2']}")
    print(f"  - 3 or more rules triggered: {overlap_counts['3_or_more']}")
    
    print("\n" + "="*50)
    print("C. ID DUPLICATE AND COLLISION ANALYSIS")
    print("="*50)
    print(f"Total memories scanned for IDs: {len(all_memory_ids)}")
    unique_mids = set(all_memory_ids)
    print(f"Unique memory IDs: {len(unique_mids)}")
    print(f"Duplicate memory IDs: {len(all_memory_ids) - len(unique_mids)}")
    
    unique_iids = set(all_incident_ids)
    print(f"Unique incident IDs: {len(unique_iids)}")
    print(f"Incident ID collisions: {len(all_incident_ids) - len(unique_iids)}")
    
if __name__ == "__main__":
    main()
