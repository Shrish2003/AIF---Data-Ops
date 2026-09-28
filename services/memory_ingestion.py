import os
import json
import uuid
import hashlib
import logging
import re
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from services.memory_service import MemoryService, OperationalMemoryRecord

logger = logging.getLogger(__name__)

# ==========================================================
# Lookups Joins Helpers
# ==========================================================
def get_lookup_context(source_system: str, record_or_entity: Dict[str, Any], parsed_lookup: Dict[str, Any], run_to_dag: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Enriches the raw entity record using its unique domain system format keys
    against lookup datasets (resolving the key re-join mismatch).
    """
    entity_id = record_or_entity.get("entity_id")
    attributes = record_or_entity.get("attributes", {})
    
    raw_dag_id = attributes.get("dag_id") or record_or_entity.get("dag_id")
    raw_run_id = entity_id or attributes.get("run_id") or record_or_entity.get("run_id")
    
    lookup_id = None
    if source_system.lower() == "airflow":
        lookup_id = raw_dag_id
        if not lookup_id and run_to_dag and raw_run_id in run_to_dag:
            lookup_id = run_to_dag[raw_run_id]
    elif source_system.lower() == "kafka":
        lookup_id = entity_id
    elif source_system.lower() in ("manufacturing", "manufacturing events"):
        lookup_id = entity_id or attributes.get("machine_id") or record_or_entity.get("machine_id")
    elif source_system.lower() in ("iot", "iot events"):
        lookup_id = entity_id or attributes.get("device_id") or record_or_entity.get("device_id")
    elif source_system.lower() in ("azure data factory", "azure_data_factory"):
        run_id = raw_run_id or attributes.get("pipelineRunId") or record_or_entity.get("pipelineRunId")
        if run_id:
            match = re.match(r"PIP(\d+)", str(run_id))
            if match:
                num = int(match.group(1))
                lookup_id = f"PIP{num:04d}"
    elif source_system.lower() in ("sap erp", "sap"):
        job_number = raw_run_id or attributes.get("job_number") or record_or_entity.get("job_number")
        if job_number:
            h = int(hashlib.md5(str(job_number).encode()).hexdigest(), 16)
            num = (h % 1000) + 1
            lookup_id = f"JOB{num:04d}"
    elif source_system.lower() == "kubernetes":
        pod_name = raw_run_id or attributes.get("pod_name") or record_or_entity.get("pod_name")
        if pod_name:
            h = int(hashlib.md5(str(pod_name).encode()).hexdigest(), 16)
            num = (h % 1000) + 1
            lookup_id = f"POD{num:04d}"
            
    if not lookup_id:
        return {}
        
    context = {
        "business_context": {},
        "incident_context": {},
        "recommendation_context": {},
        "lineage_context": {}
    }
    
    # Join on business_context
    for r in parsed_lookup.get("business_context", []):
        if r.get("entity_id") == lookup_id:
            context["business_context"] = r
            break
            
    # Join on incident_history
    for r in parsed_lookup.get("incident_history", []):
        if r.get("entity_id") == lookup_id:
            context["incident_context"] = r
            break
            
    # Join on recommendation_history
    for r in parsed_lookup.get("recommendation_history", []):
        if r.get("entity_id") == lookup_id:
            context["recommendation_context"] = r
            break
            
    # Join on pipeline_lineage
    for r in parsed_lookup.get("pipeline_lineage", []):
        if r.get("entity_id") == lookup_id:
            context["lineage_context"] = r
            break
            
    return context


# ==========================================================
# Operational Incident Extractor Implementation
# ==========================================================
class OperationalIncidentExtractor:
    """
    Extracts real operational incidents from canonical operational data and agent outputs.
    Ensures that only current failure/anomaly states qualify as incidents, and maps
    their fields accurately without fabrication.
    """
    
    def __init__(self, parsed_lookup: Dict[str, Any], run_to_dag: Optional[Dict[str, str]] = None):
        self.parsed_lookup = parsed_lookup
        self.run_to_dag = run_to_dag or {}
        
    def qualify_incident(self, entity: Dict[str, Any], obs: Dict[str, Any], beh: Dict[str, Any], risk: Dict[str, Any], integ: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Qualifies if the entity represents a current operational incident.
        Does NOT use historical context as a standalone trigger.
        """
        execution_status = entity.get("execution_status")
        
        # 1. Current pipeline run failure
        if execution_status in ("FAILED", "CANCELLED"):
            return True, "execution_status_failure"
            
        # 2. Behavioral anomaly severity
        if beh.get("behavior", {}).get("severity") in ("WARNING", "CRITICAL"):
            return True, "behavior_severity_anomaly"
            
        # 3. Risk prediction trigger
        if risk.get("risk_severity") in ("HIGH", "CRITICAL") or risk.get("risk_score", 0.0) >= 70.0:
            return True, "risk_severity_trigger"
            
        # 4. Data integrity validation status
        if integ.get("integrity_status") == "FAILED":
            return True, "integrity_failure"
            
        # 5. Observer events or deviations
        if obs.get("observations", {}).get("events"):
            return True, "observer_events"
            
        baseline = obs.get("observations", {}).get("baseline", {})
        for metric, details in baseline.items():
            if isinstance(details, dict) and details.get("status") in ("WARNING", "CRITICAL"):
                return True, "observer_baseline_deviation"
                
        return False, None

    def extract_record(self, entity: Dict[str, Any], obs: Dict[str, Any], beh: Dict[str, Any], risk: Dict[str, Any], integ: Dict[str, Any], rec: Dict[str, Any], ds_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Maps a qualified operational entity to the strict schema dictionary.
        Does not fabricate any missing fields.
        """
        source_system = entity.get("source_system", "Unknown")
        entity_id = entity.get("entity_id")
        entity_type = entity.get("entity_type")
        attributes = entity.get("attributes", {})
        event_timestamp = entity.get("event_timestamp") or datetime.now().isoformat()
        execution_status = entity.get("execution_status", "UNKNOWN")
        
        # Qualify trigger type
        _, trigger_type = self.qualify_incident(entity, obs, beh, risk, integ)
        
        if trigger_type == "execution_status_failure":
            incident_type = f"{entity_type} Execution Failure"
        elif trigger_type == "behavior_severity_anomaly":
            incident_type = "Behavioral Anomaly"
        elif trigger_type == "risk_severity_trigger":
            incident_type = "High Risk Prediction"
        elif trigger_type == "integrity_failure":
            incident_type = "Data Integrity Violation"
        elif trigger_type in ("observer_events", "observer_baseline_deviation"):
            incident_type = "Telemetry Deviation Anomaly"
        else:
            incident_type = "Operational Incident"
            
        # Deterministic incident ID
        raw_run_id = attributes.get("run_id") or attributes.get("pipelineRunId") or attributes.get("job_number")
        if raw_run_id:
            incident_id = str(raw_run_id)
        else:
            incident_id = f"{source_system}_{entity_id}_{event_timestamp}_{incident_type}"
            
        # Deterministic memory ID
        unique_key = f"{source_system}_{incident_id}"
        hashed = hashlib.md5(unique_key.encode()).hexdigest()[:8]
        memory_id = f"mem_{hashed}"
        
        # Populate pipeline_id ONLY if entity is a Pipeline
        if entity_type == "Pipeline":
            if source_system.lower() == "airflow":
                dag_id = attributes.get("dag_id")
                if not dag_id and self.run_to_dag and entity_id in self.run_to_dag:
                    dag_id = self.run_to_dag[entity_id]
                pipeline_id = dag_id or "Not available"
            else:
                pipeline_id = entity.get("entity_name", "Not available")
        else:
            pipeline_id = "Not available"
            
        # Lookup context re-join
        context = get_lookup_context(source_system, entity, self.parsed_lookup, self.run_to_dag)
        bus_ctx = context.get("business_context", {})
        inc_ctx = context.get("incident_context", {})
        rec_ctx = context.get("recommendation_context", {})
        lin_ctx = context.get("lineage_context", {})
        
        # Ingress incident details
        summary = f"{source_system} {entity_type} '{entity.get('entity_name')}' (ID: {entity_id}) status: {execution_status}."
        if trigger_type == "execution_status_failure":
            summary += " Reason: Execution failure state detected."
        elif trigger_type == "behavior_severity_anomaly":
            summary += f" Reason: Behavioral anomaly flagged (Severity: {beh.get('behavior', {}).get('severity')})."
        elif trigger_type == "risk_severity_trigger":
            summary += f" Reason: High risk prediction scored at {risk.get('risk_score', 0.0)}%."
            
        symptom_parts = []
        for key, value in attributes.items():
            if key not in ("run_id", "pipelineRunId", "job_number", "dag_id", "dag_name", "task_id"):
                symptom_parts.append(f"{key}: {value}")
        symptoms = ", ".join(symptom_parts) if symptom_parts else "No specific telemetry symptoms available."
        
        behavior_text = "Not available"
        if beh:
            beh_analysis = beh.get("behavior", {})
            if beh_analysis:
                beh_sev = beh_analysis.get("severity")
                beh_patterns = beh_analysis.get("patterns", [])
                behavior_text = f"Severity: {beh_sev}. Patterns: {', '.join(beh_patterns)}" if beh_patterns else f"Severity: {beh_sev}."
                
        risk_text = "Not available"
        if risk:
            risk_text = f"Severity: {risk.get('risk_severity')}. Score: {risk.get('risk_score')}. Category: {risk.get('risk_category')}."
            
        affected_services = []
        downstream = lin_ctx.get("downstream")
        if downstream:
            if isinstance(downstream, list):
                affected_services = downstream
            else:
                affected_services = [s.strip() for s in str(downstream).split(",") if s.strip()]
                
        business_impact = "Not available"
        if bus_ctx:
            business_impact = f"Business Unit: {bus_ctx.get('business_unit')}. Criticality: {bus_ctx.get('criticality')}. Application: {bus_ctx.get('application')}."
            
        recommendation_text = "Not available"
        if rec and rec.get("recommendation"):
            recommendation_text = f"Action: {rec.get('recommendation')}. Reason: {rec.get('reason')}."
        elif rec_ctx and rec_ctx.get("recommendation"):
            recommendation_text = f"Action: {rec_ctx.get('recommendation')}. Expected Impact: {rec_ctx.get('impact')}."
            
        root_cause = inc_ctx.get("root_cause") or "Not available"
        action_taken = inc_ctx.get("resolution") or "Not available"
        
        outcome = "Not available"
        if rec_ctx and rec_ctx.get("impact"):
            outcome = f"Applied: {rec_ctx.get('applied')}. Historical Success Rate: {rec_ctx.get('success_rate')}%."
            
        # Refinement 2: resolution_status
        if execution_status == "FAILED":
            resolution_status = "PENDING"
        else:
            resolution_status = "Not determined"
            
        record_dict = {
            "memory_id": memory_id,
            "incident_id": incident_id,
            "pipeline_id": pipeline_id,
            "timestamp": event_timestamp,
            "incident_type": incident_type,
            "summary": summary,
            "symptoms": symptoms,
            "behavior": behavior_text,
            "root_cause": root_cause,
            "risk": risk_text,
            "affected_services": affected_services,
            "business_impact": business_impact,
            "recommendation": recommendation_text,
            "action_taken": action_taken,
            "outcome": outcome,
            "resolution_status": resolution_status
        }
        
        # Source traceability fields to be saved separately in ChromaDB metadata
        record_dict["source_system"] = source_system
        record_dict["source_dataset"] = ds_name or entity.get("dataset_name") or "Unknown"
        record_dict["source_record_id"] = incident_id
        record_dict["source_pipeline_id"] = pipeline_id
        record_dict["source_timestamp"] = event_timestamp
        record_dict["created_at"] = datetime.now().isoformat()
        record_dict["memory_version"] = "2.0"
        
        return record_dict


# ==========================================================
# Ingestion API Entry Point
# ==========================================================
def ingest_real_operational_data(max_records: Optional[int] = None, collection_name: str = "operational_memory", execution_output_path: str = "output/execution_output.json") -> Dict[str, Any]:
    """
    Ingests AIF real execution telemetry records into persistent Operational Memory.
    Cautions duplicate entries and logs counts accurately.
    """
    if not os.path.exists(execution_output_path):
        raise FileNotFoundError(
            f"Execution output file '{execution_output_path}' not found. Please run the Capability Adapter "
            "first to generate the execution outputs."
        )
        
    with open(execution_output_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    adapter_output = data.get("adapter_output", {})
    parsed_lookup = adapter_output.get("parsed_lookup", {})
    parsed_raw = adapter_output.get("parsed_raw", {})
    
    # Resolve join keys mapping mismatch for Airflow
    raw_airflow = parsed_raw.get("airflow", [])
    run_to_dag = {r.get("run_id"): r.get("dag_id") for r in raw_airflow if r.get("run_id") and r.get("dag_id")}
    
    op_ent = data.get("operational_entity", {})
    obs_out = data.get("observer_output", {})
    beh_out = data.get("behavior_output", {})
    risk_out = data.get("risk_output", {})
    integ_out = data.get("integrity_output", {})
    rec_out = data.get("recommendation_output", {})
    
    extractor = OperationalIncidentExtractor(parsed_lookup, run_to_dag)
    
    stats = {
        "source_records_scanned": 0,
        "qualifying_incidents": 0,
        "inserted": 0,
        "duplicates_skipped": 0,
        "rejected_records": 0,
        "missing_fields": 0,
        "embedding_failures": 0,
        "chromadb_failures": 0
    }
    
    service = MemoryService(collection_name=collection_name)
    if not service.is_available:
        logger.error("ChromaDB is not available for ingestion.")
        stats["chromadb_failures"] = 1
        return stats
        
    qualified_records_by_ds = {ds_name: [] for ds_name in op_ent.keys()}
    
    for ds_name, entities in op_ent.items():
        stats["source_records_scanned"] += len(entities)
        
        obs_map = {x["entity_id"]: x for x in obs_out.get(ds_name, []) if "entity_id" in x}
        beh_map = {x["entity_id"]: x for x in beh_out.get(ds_name, []) if "entity_id" in x}
        risk_map = {x["entity_id"]: x for x in risk_out.get(ds_name, []) if "entity_id" in x}
        integ_map = {x["entity_id"]: x for x in integ_out.get(ds_name, []) if "entity_id" in x}
        rec_map = {x["entity_id"]: x for x in rec_out.get(ds_name, []) if "entity_id" in x}
        
        for entity in entities:
            eid = entity.get("entity_id")
            if not eid:
                stats["rejected_records"] += 1
                continue
                
            obs_obj = obs_map.get(eid, {})
            beh_obj = beh_map.get(eid, {})
            risk_obj = risk_map.get(eid, {})
            integ_obj = integ_map.get(eid, {})
            rec_obj = rec_map.get(eid, {})
            
            is_incident, _ = extractor.qualify_incident(entity, obs_obj, beh_obj, risk_obj, integ_obj)
            if is_incident:
                stats["qualifying_incidents"] += 1
                
                try:
                    record_dict = extractor.extract_record(entity, obs_obj, beh_obj, risk_obj, integ_obj, rec_obj, ds_name=ds_name)
                    qualified_records_by_ds[ds_name].append(record_dict)
                except Exception as e:
                    logger.warning(f"Failed to extract incident for {eid}: {e}")
                    stats["rejected_records"] += 1
                    
    # Compile a balanced list of records to insert
    to_insert = []
    if max_records is not None:
        num_datasets = len(qualified_records_by_ds)
        per_ds_limit = max(1, max_records // num_datasets)
        for ds_name, records in qualified_records_by_ds.items():
            to_insert.extend(records[:per_ds_limit])
        to_insert = to_insert[:max_records]
    else:
        for ds_name, records in qualified_records_by_ds.items():
            to_insert.extend(records)
        
    for record in to_insert:
        mem_id = record["memory_id"]
        
        # Check if already exists in collection
        try:
            existing = service.get_memory(mem_id)
            if existing:
                stats["duplicates_skipped"] += 1
                continue
        except Exception:
            pass
            
        try:
            res_id = service.add_memory(record)
            if res_id:
                stats["inserted"] += 1
            else:
                stats["chromadb_failures"] += 1
        except Exception as e:
            if "Ollama" in str(e) or "embedding" in str(e).lower() or "connection" in str(e).lower():
                stats["embedding_failures"] += 1
            else:
                stats["chromadb_failures"] += 1
                
    return stats
