import os
import json

def main():
    path = "output/execution_output.json"
    if not os.path.exists(path):
        print(f"File {path} does not exist!")
        return
        
    print(f"Loading {path}...")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    op_ent = data.get("operational_entity", {})
    obs_out = data.get("observer_output", {})
    beh_out = data.get("behavior_output", {})
    risk_out = data.get("risk_output", {})
    integ_out = data.get("integrity_output", {})
    
    total_scanned = 0
    qualified_incidents = 0
    reasons = {}
    
    for ds_name, entities in op_ent.items():
        total_scanned += len(entities)
        
        # Build maps for quick lookup by entity_id
        obs_map = {x["entity_id"]: x for x in obs_out.get(ds_name, []) if "entity_id" in x}
        beh_map = {x["entity_id"]: x for x in beh_out.get(ds_name, []) if "entity_id" in x}
        risk_map = {x["entity_id"]: x for x in risk_out.get(ds_name, []) if "entity_id" in x}
        integ_map = {x["entity_id"]: x for x in integ_out.get(ds_name, []) if "entity_id" in x}
        
        for entity in entities:
            eid = entity.get("entity_id")
            if not eid:
                continue
                
            execution_status = entity.get("execution_status")
            
            # Fetch matching agent outputs
            obs_obj = obs_map.get(eid, {})
            beh_obj = beh_map.get(eid, {})
            risk_obj = risk_map.get(eid, {})
            integ_obj = integ_map.get(eid, {})
            
            # Incident qualification check
            is_incident = False
            trigger = None
            
            # 1. Execution status failure
            if execution_status in ("FAILED", "CANCELLED"):
                is_incident = True
                trigger = "execution_status_failure"
                
            # 2. Behavior anomaly
            elif beh_obj.get("behavior", {}).get("severity") in ("WARNING", "CRITICAL"):
                is_incident = True
                trigger = "behavior_severity_anomaly"
                
            # 3. Risk prediction trigger
            elif risk_obj.get("risk_severity") in ("HIGH", "CRITICAL") or risk_obj.get("risk_score", 0.0) >= 70.0:
                is_incident = True
                trigger = "risk_severity_trigger"
                
            # 4. Integrity validation failure
            elif integ_obj.get("integrity_status") in ("FAIL", "WARNING") or integ_obj.get("primary_failure_reason"):
                is_incident = True
                trigger = "integrity_failure"
                
            # 5. Observer events/anomalies
            elif obs_obj.get("observations", {}).get("events"):
                is_incident = True
                trigger = "observer_events"
            else:
                # Check baseline comparison warning/critical
                baseline = obs_obj.get("observations", {}).get("baseline", {})
                for metric, details in baseline.items():
                    if isinstance(details, dict) and details.get("status") in ("WARNING", "CRITICAL"):
                        is_incident = True
                        trigger = "observer_baseline_deviation"
                        break
                        
            if is_incident:
                qualified_incidents += 1
                reasons[trigger] = reasons.get(trigger, 0) + 1
                
    print(f"\nTotal scanned records: {total_scanned}")
    print(f"Qualifying incidents: {qualified_incidents}")
    print("Qualification triggers breakdown:")
    for trigger, count in reasons.items():
        print(f"  - {trigger}: {count}")

if __name__ == "__main__":
    main()
