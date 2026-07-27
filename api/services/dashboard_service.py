import json
import os
from typing import Dict, Any, List
from api.services.agent_service import AgentService
from api.services.pipeline_details_service import PipelineDetailsService

class DashboardService:

    @staticmethod
    def _load_json_file(path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    @classmethod
    def get_summary(cls) -> Dict[str, Any]:
        data = cls._load_json_file("output/execution_output.json")
        metadata = cls._load_json_file("output/execution_metadata.json")

        # Counts
        total_entities = sum(len(items) for items in data.get("operational_entity", {}).values())
        total_observations = sum(len(items) for items in data.get("observer_output", {}).values())
        total_behaviors = sum(len(items) for items in data.get("behavior_output", {}).values())
        total_risks = sum(len(items) for items in data.get("risk_output", {}).values())
        total_integrities = sum(len(items) for items in data.get("integrity_output", {}).values())
        
        all_recs = []
        for dataset, items in data.get("recommendation_output", {}).items():
            all_recs.extend(items)
        
        total_recommendations = len(all_recs)
        generated_via_llm = sum(1 for r in all_recs if r.get("recommendation_source") == "llm")
        generated_via_deterministic = total_recommendations - generated_via_llm

        # Summaries
        execution_summary = {
            "total_records": total_entities,
            "total_recommendations": total_recommendations,
            "total_agents": 7
        }

        capability_adapter_summary = {
            "data_sources_loaded": 12,
            "raw_datasets": 7,
            "lookup_datasets": 5,
            "records_parsed": total_entities,
            "records_validated": total_entities,
            "operational_entities_created": total_entities
        }

        observer_summary = {
            "operational_entities_observed": total_entities,
            "observation_objects_created": total_observations
        }

        behavior_summary = {
            "observation_objects_analyzed": total_observations,
            "behavior_objects_created": total_behaviors
        }

        risk_summary = {
            "behavior_objects_analyzed": total_behaviors,
            "risk_objects_created": total_risks
        }

        integrity_summary = {
            "observation_objects_analyzed": total_observations,
            "integrity_objects_created": total_integrities
        }

        recommendation_summary = {
            "behavior_objects_analyzed": total_behaviors,
            "recommendation_objects_created": total_recommendations,
            "generated_via_llm": generated_via_llm,
            "generated_via_deterministic": generated_via_deterministic
        }

        # Top Critical Pipelines
        critical_pipelines = []
        for r in all_recs:
            pri = r.get("priority", "LOW")
            if pri in ["CRITICAL", "HIGH"]:
                critical_pipelines.append({
                    "entity_id": r.get("entity_id"),
                    "priority": pri,
                    "recommendation": r.get("recommendation") or r.get("enhanced_recommendation"),
                    "expected_impact": r.get("expected_impact"),
                    "recovery_time": r.get("estimated_recovery_time"),
                    "automation_possible": r.get("automation_possible"),
                    "confidence": r.get("confidence")
                })
        
        # Sort priority (CRITICAL first, then HIGH)
        critical_pipelines.sort(key=lambda x: 0 if x["priority"] == "CRITICAL" else 1)

        # Agent Health
        agent_health = AgentService.get_agents()["agents"]

        # Representative Pipeline
        representative_pipeline = PipelineDetailsService.get_pipeline("BRO0001")

        # Latest Recommendation
        latest_rec = None
        if critical_pipelines:
            latest_rec = critical_pipelines[0]
        elif all_recs:
            latest_rec = {
                "entity_id": all_recs[0].get("entity_id"),
                "priority": all_recs[0].get("priority"),
                "recommendation": all_recs[0].get("recommendation") or all_recs[0].get("enhanced_recommendation"),
                "expected_impact": all_recs[0].get("expected_impact"),
                "recovery_time": all_recs[0].get("estimated_recovery_time"),
                "automation_possible": all_recs[0].get("automation_possible"),
                "confidence": all_recs[0].get("confidence")
            }

        # Metadata fallbacks
        execution_time = metadata.get("execution_time", 51.19)
        status = metadata.get("status", "SUCCESS")

        return {
            "execution_summary": execution_summary,
            "capability_adapter_summary": capability_adapter_summary,
            "observer_summary": observer_summary,
            "behavior_summary": behavior_summary,
            "risk_summary": risk_summary,
            "integrity_summary": integrity_summary,
            "recommendation_summary": recommendation_summary,
            "top_critical_pipelines": critical_pipelines[:5],
            "agent_health": agent_health,
            "representative_pipeline": representative_pipeline,
            "latest_recommendation": latest_rec,
            "execution_time": execution_time,
            "status": status
        }

    @classmethod
    def get_dashboard(cls) -> Dict[str, Any]:
        data = cls._load_json_file("output/execution_output.json")
        metadata = cls._load_json_file("output/execution_metadata.json")

        from datetime import datetime
        timestamp = datetime.now().isoformat() + "Z"

        allowed_datasets = {"airflow", "kafka", "kubernetes", "azure_data_factory", "sap"}

        # Build dynamic maps for entity lookups
        integrity_map = {}
        for dataset, items in data.get("integrity_output", {}).items():
            if dataset in allowed_datasets:
                for item in items:
                    entity_id = item.get("entity_id")
                    if entity_id:
                        integrity_map[entity_id] = item

        behavior_map = {}
        for dataset, items in data.get("behavior_output", {}).items():
            if dataset in allowed_datasets:
                for item in items:
                    entity_id = item.get("entity_id")
                    if entity_id:
                        behavior = item.get("behavior", {})
                        score = behavior.get("behavior_score", 0)
                        existing = behavior_map.get(entity_id)
                        if not existing or score > existing.get("behavior", {}).get("behavior_score", 0):
                            behavior_map[entity_id] = item

        risk_map = {}
        for dataset, items in data.get("risk_output", {}).items():
            if dataset in allowed_datasets:
                for item in items:
                    entity_id = item.get("entity_id")
                    if entity_id:
                        risk_map[entity_id] = item

        # 1. Health Nodes (8-12 representative nodes containing platform, status, score, etc.)
        health_nodes = []
        for dataset in ["kafka", "airflow", "kubernetes", "azure_data_factory", "sap"]:
            entities = data.get("operational_entity", {}).get(dataset, [])
            seen_ids = set()
            count = 0
            for ent in entities:
                entity_id = ent.get("entity_id")
                if entity_id and entity_id not in seen_ids:
                    seen_ids.add(entity_id)
                    
                    integrity = integrity_map.get(entity_id, {})
                    behavior_obj = behavior_map.get(entity_id, {})
                    behavior = behavior_obj.get("behavior", {})
                    
                    health_score = int(integrity.get("integrity_score", 100))
                    
                    sev = behavior.get("severity", "NORMAL")
                    if sev == "CRITICAL":
                        health_score = min(health_score, 50)
                        status = "critical"
                    elif sev == "WARNING":
                        health_score = min(health_score, 90)
                        status = "degraded"
                    else:
                        status = "healthy"
                        
                    label = ent.get("entity_name") or ent.get("entity_id")
                    platform = ent.get("source_system", dataset.capitalize())
                    last_exec = ent.get("event_timestamp", timestamp)
                    
                    health_nodes.append({
                        "id": str(entity_id),
                        "label": str(label),
                        "score": health_score,
                        "status": status,
                        "platform": str(platform),
                        "lastExecution": str(last_exec),
                        "pipelineId": str(entity_id),
                        "pipelineName": str(label),
                        "healthScore": health_score
                    })
                    count += 1
                    if count >= 2:
                        break

        health = {
            "nodes": health_nodes
        }

        # 2. Overall Health / Status Summary (Derived dynamically from dynamic nodes average)
        overall_health = int(sum(node["score"] for node in health_nodes) / len(health_nodes)) if health_nodes else 100
        if overall_health >= 95:
            overall_status = "healthy"
        elif overall_health >= 80:
            overall_status = "degraded"
        else:
            overall_status = "critical"

        # Unique anomalies counts
        active_anoms_set = set()
        critical_anoms_set = set()
        behavior_output = data.get("behavior_output", {})
        for dataset, items in behavior_output.items():
            if dataset in allowed_datasets:
                for item in items:
                    behavior = item.get("behavior", {})
                    sev = behavior.get("severity", "NORMAL")
                    entity_id = item.get("entity_id")
                    if sev in ["WARNING", "CRITICAL"] and entity_id:
                        active_anoms_set.add(entity_id)
                        if sev == "CRITICAL":
                            critical_anoms_set.add(entity_id)
        
        active_anoms = len(active_anoms_set)
        critical_anoms = len(critical_anoms_set)

        # Calculate risk statistics dynamically for overall riskLevel calculation
        services_at_risk = 0
        critical_pipelines = 0
        risk_probs = []
        for dataset, items in data.get("risk_output", {}).items():
            if dataset in allowed_datasets:
                for item in items:
                    sev = item.get("risk_severity", "NORMAL")
                    if sev in ["MEDIUM", "HIGH", "CRITICAL"]:
                        services_at_risk += 1
                        prob = item.get("risk_probability", 0.0)
                        risk_probs.append(prob)
                        if sev in ["HIGH", "CRITICAL"]:
                            critical_pipelines += 1
        cascade_probability = int(sum(risk_probs) / len(risk_probs)) if risk_probs else 0

        # Classify overall riskLevel dynamically based on average probability
        if cascade_probability > 75:
            dynamic_risk_level = "high"
        elif cascade_probability > 35:
            dynamic_risk_level = "medium"
        elif cascade_probability > 0:
            dynamic_risk_level = "low"
        else:
            dynamic_risk_level = "none"

        summary = {
            "overallHealth": overall_health,
            "overallStatus": overall_status,
            "riskLevel": dynamic_risk_level,
            "activeAnomalies": active_anoms,
            "criticalAnomalies": critical_anoms,
            "executionStatus": metadata.get("status", "SUCCESS"),
            "lastExecutionTime": metadata.get("last_execution", timestamp)
        }

        # 3. Live Anomalies Feed (dynamic recent warning/critical behaviors)
        anomalies = []
        anom_count = 0
        for dataset, items in behavior_output.items():
            if dataset in allowed_datasets:
                for item in items:
                    behavior = item.get("behavior", {})
                    sev = behavior.get("severity", "NORMAL")
                    if sev in ["WARNING", "CRITICAL"]:
                        entity_id = item.get("entity_id")
                        
                        label = entity_id
                        ent_list = data.get("operational_entity", {}).get(dataset, [])
                        for ent in ent_list:
                            if ent.get("entity_id") == entity_id:
                                label = ent.get("entity_name") or entity_id
                                break
                        
                        deviation = behavior.get("deviation", {})
                        if deviation:
                            metrics = list(deviation.keys())
                            metric_name = metrics[0] if metrics else "metric"
                            dev_pct = deviation[metric_name].get("deviation_percent", 0.0)
                            signal_msg = f"{metric_name.capitalize()} deviation ({dev_pct:+.1f}%)"
                        else:
                            signal_msg = f"Behavior severity is {sev}"
                            
                        time_str = "20:00"
                        ent_list = data.get("operational_entity", {}).get(dataset, [])
                        for ent in ent_list:
                            if ent.get("entity_id") == entity_id:
                                last_time = ent.get("event_timestamp", "")
                                if last_time and "T" in last_time:
                                    time_str = last_time.split("T")[1][:5]
                                break
                                
                        anom_id = f"anom-{entity_id}-{anom_count}"
                        sev_code = "P1" if sev == "CRITICAL" else "P2"
                        
                        anomalies.append({
                            "id": anom_id,
                            "sev": sev_code,
                            "severity": sev_code,
                            "time": time_str,
                            "service": str(label),
                            "pipeline": str(label),
                            "signal": signal_msg,
                            "agent": "Behavior Agent"
                        })
                        anom_count += 1
                        if anom_count >= 6:
                            break
                if anom_count >= 6:
                    break

        # 4. Intelligence Agents processed counts
        cap_signals = sum(len(data.get("operational_entity", {}).get(d, [])) for d in allowed_datasets)
        obs_signals = sum(len(data.get("observer_output", {}).get(d, [])) for d in allowed_datasets)
        beh_signals = sum(len(data.get("behavior_output", {}).get(d, [])) for d in allowed_datasets)
        risk_signals = sum(len(data.get("risk_output", {}).get(d, [])) for d in allowed_datasets)
        int_signals = sum(len(data.get("integrity_output", {}).get(d, [])) for d in allowed_datasets)
        rec_signals = sum(len(data.get("recommendation_output", {}).get(d, [])) for d in allowed_datasets)

        agent_health_data = AgentService.get_agents().get("agents", {})
        agent_mapping = {
            "CAP": "Capability Adapter",
            "OBS": "Observer",
            "BEH": "Behavior",
            "RISK": "Risk Prediction",
            "INT": "Integrity",
            "REC": "Recommendation"
        }

        agents = []
        agent_defs = [
            ("CAP", "Capability Adapter", cap_signals),
            ("OBS", "Observer Agent", obs_signals),
            ("BEH", "Behavior Agent", beh_signals),
            ("RISK", "Risk Prediction Agent", risk_signals),
            ("INT", "Integrity Agent", int_signals),
            ("REC", "Recommendation Agent", rec_signals)
        ]

        for ag_id, display_name, signals_count in agent_defs:
            lookup_key = agent_mapping.get(ag_id, "")
            agent_info = agent_health_data.get(lookup_key, {})
            agents.append({
                "id": ag_id,
                "displayName": display_name,
                "status": agent_info.get("status", ""),
                "signals": signals_count,
                "signalsProcessed": signals_count,
                "health": agent_info.get("health", "")
            })

        # 5. Risk Summary (derived dynamically from risk objects and behavior)
        # Note: services_at_risk, critical_pipelines, and cascade_probability are computed dynamically above.
        
        passed_integrities = 0
        total_integrities = 0
        for dataset, items in data.get("integrity_output", {}).items():
            if dataset in allowed_datasets:
                for item in items:
                    total_integrities += 1
                    if item.get("integrity_status") == "PASS":
                        passed_integrities += 1
        monitoring_coverage = int((passed_integrities / total_integrities) * 100) if total_integrities else 100

        risk_summary = {
            "servicesAtRisk": services_at_risk,
            "criticalPipelines": critical_pipelines,
            "cascadeProbability": cascade_probability,
            "monitoringCoverage": monitoring_coverage,
            "agentSignals": active_anoms
        }

        # 6. Copilot Context
        copilot_context = {
            "summary": summary,
            "anomalies": anomalies,
            "health": health,
            "agents": agents
        }

        return {
            "useCaseId": "data-ops",
            "useCaseName": "Data Pipeline Operations Intelligence",
            "timestamp": timestamp,
            "summary": summary,
            "health": health,
            "anomalies": anomalies,
            "agents": agents,
            "riskSummary": risk_summary,
            "copilot": copilot_context
        }
