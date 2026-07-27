import logging
import json
import os
import time

from dotenv import load_dotenv
load_dotenv()

from adapters.connectors.csv_connector import CSVConnector
from adapters.parsers.tabular_parser import TabularParser
from adapters.mapping.mapping_engine import MappingEngine
from adapters.normalizer.normalizer import Normalizer
from adapters.enricher.context_enricher import ContextEnricher
from adapters.validation.validation_engine import ValidationEngine
from adapters.entity_builder.operational_entity_builder import OperationalEntityBuilder
from agents.observer.observer_agent import ObserverAgent
from agents.behavior.behavior_agent import BehaviorAgent
from agents.risk.risk_agent import RiskPredictionAgent
from agents.integrity.integrity_agent import IntegrityAgent
from agents.recommendation.recommendation_agent import RecommendationAgent

# ==========================================================
# Logging Configuration
# ==========================================================

LOG_LEVEL = os.getenv("AIF_LOG_LEVEL", "WARNING").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(message)s"
)

logger = logging.getLogger(__name__)

# ==========================================================
# MAIN
# ==========================================================
SUPPORTED_DATA_PIPELINE_PLATFORMS = {
    "airflow",
    "kafka",
    "kubernetes",
    "azure_data_factory",
    "sap",
    "sap erp"
}

def select_representative_pipeline_from_results(operational_entities, behavior_objects, risk_objects, integrity_objects, recommendation_agent):
    engine = recommendation_agent.recommendation_engine
    entities = {}
    
    for ds_name, items in operational_entities.items():
        # Normalize name for case-insensitive checking
        ds_normalized = ds_name.strip().lower()
        if ds_normalized not in SUPPORTED_DATA_PIPELINE_PLATFORMS:
            continue
            
        behaviors = behavior_objects.get(ds_name, [])
        risks = risk_objects.get(ds_name, [])
        integrities = integrity_objects.get(ds_name, [])
        
        beh_map = {x.get("entity_id"): x for x in behaviors}
        risk_map = {x.get("entity_id"): x for x in risks}
        integ_map = {x.get("entity_id"): x for x in integrities}
        
        for entity in items:
            eid = entity.get("entity_id")
            if not eid:
                continue
                
            beh_obj = beh_map.get(eid, {})
            risk_obj = risk_map.get(eid, {})
            integ_obj = integ_map.get(eid, {})
            
            context = engine.analyze(beh_obj, risk_obj, integ_obj, entity)
            priority = context.get("priority", "LOW")
            risk_score = risk_obj.get("risk_score", 0.0)
            
            entities[eid] = {
                "entity_id": eid,
                "entity_name": entity.get("entity_name"),
                "platform": entity.get("source_system"),
                "priority": priority,
                "risk_score": risk_score,
                "behavior_severity": beh_obj.get("behavior", {}).get("severity"),
                "integrity_status": integ_obj.get("integrity_status"),
                "dataset_name": ds_name,
                "op_entity": entity,
                "behavior": beh_obj,
                "risk": risk_obj,
                "integrity": integ_obj
            }
            
    all_entities = list(entities.values())
    critical_entities = [e for e in all_entities if e["priority"] == "CRITICAL"]
    high_entities = [e for e in all_entities if e["priority"] == "HIGH"]
    
    selected = None
    reason = ""
    
    if critical_entities:
        critical_entities.sort(key=lambda x: x["risk_score"], reverse=True)
        selected = critical_entities[0]
        reason = "Highest Priority Recommendation (CRITICAL)"
    elif high_entities:
        high_entities.sort(key=lambda x: x["risk_score"], reverse=True)
        selected = high_entities[0]
        reason = "Highest Priority Recommendation (HIGH)"
    else:
        all_entities.sort(key=lambda x: x["risk_score"], reverse=True)
        if all_entities and all_entities[0]["risk_score"] > 0.0:
            selected = all_entities[0]
            reason = "Highest Risk Score"
        else:
            for platform_name in ["airflow", "kafka", "kubernetes", "azure_data_factory", "sap"]:
                items = operational_entities.get(platform_name, [])
                if items:
                    first_eid = items[0].get("entity_id")
                    if first_eid in entities:
                        selected = entities[first_eid]
                        reason = "First Available Pipeline"
                        break
                        
    if not selected and all_entities:
        selected = all_entities[0]
        reason = "First Available Pipeline"
        
    return selected, reason


def print_refined_execution_flow(selected_pipeline, selection_reason, parsed_raw, parsed_lookup, validation_results, operational_entities, observation_objects, behavior_objects, risk_objects, integrity_objects, recommendation_objects, recommendation_agent, execution_time, output_path):
    representative_id = selected_pipeline["entity_id"]
    representative_dataset = selected_pipeline["dataset_name"]
    
    print("==================================================")
    print("Representative Pipeline Selected")
    print("==================================================")
    print(f"Pipeline ID             : {representative_id}")
    print(f"Pipeline Name           : {selected_pipeline['entity_name']}")
    print(f"Platform                : {selected_pipeline['platform']}")
    print(f"Selection Reason        : {selection_reason}")
    print("Decision Factors        :")
    print(f"  • Recommendation Priority : {selected_pipeline['priority']}")
    print(f"  • Risk Score              : {selected_pipeline['risk_score']}")
    print(f"  • Behavior Severity       : {selected_pipeline['behavior_severity']}")
    print(f"  • Integrity Status        : {selected_pipeline['integrity_status']}")
    print("==================================================")
    print()
    
    representative_op_entity = selected_pipeline["op_entity"]
    representative_observation = next(
        obs for obs in observation_objects[representative_dataset]
        if obs.get("entity_id") == representative_id
    )
    representative_behavior = next(
        beh for beh in behavior_objects[representative_dataset]
        if beh.get("entity_id") == representative_id
    )
    representative_risk = next(
        risk for risk in risk_objects[representative_dataset]
        if risk.get("entity_id") == representative_id
    )
    representative_integrity = next(
        integ for integ in integrity_objects[representative_dataset]
        if integ.get("entity_id") == representative_id
    )
    representative_recommendation = next(
        rec for rec in recommendation_objects[representative_dataset]
        if rec.get("entity_id") == representative_id
    )
    
    def print_arrow():
        print()
        try:
            print("                                       ↓")
        except UnicodeEncodeError:
            print("                                       v")
        print()

    # CAPABILITY ADAPTER
    print("==================================================")
    print(f"{'Capability Adapter':^50}")
    print("==================================================")
    print(f"Pipeline ID             : {representative_id}")
    print(f"Platform                : {representative_op_entity.get('source_system')}")
    print(f"Execution Status        : {representative_op_entity.get('execution_status')}")
    print(f"Timestamp               : {representative_op_entity.get('event_timestamp')}")
    
    records_parsed = sum(len(records) for records in parsed_raw.values())
    lookup_parsed = sum(len(records) for records in parsed_lookup.values())
    validated_records = sum(len(records) for records in validation_results.values())
    total_entities = sum(len(v) for v in operational_entities.values())
    
    print("Overall Statistics      :")
    print(f"  • Records Parsed      : {records_parsed}")
    print(f"  • Lookup Records      : {lookup_parsed}")
    print(f"  • Records Validated   : {validated_records}")
    print(f"  • Entities Created    : {total_entities}")
    print("==================================================")
    
    # OBSERVER
    print_arrow()
    print("==================================================")
    print(f"{'Observer':^50}")
    print("==================================================")
    print(f"Pipeline ID             : {representative_id}")
    obs_events = representative_observation.get("observations", {}).get("events", [])
    obs_baseline = representative_observation.get("observations", {}).get("baseline", {})
    obs_trend = representative_observation.get("observations", {}).get("trend", {})
    obs_metrics = representative_observation.get("observations", {}).get("metrics", {})
    
    obs_state = derive_observation_status(obs_events, obs_baseline)
    obs_reasons = summarize_observer_reason(obs_events, obs_baseline, obs_trend)
    
    print("Observed Metrics        :")
    for m_name, m_val in obs_metrics.items():
        print(f"  • {m_name}: {m_val}")
    print("Detected Event          :")
    if obs_events:
        for ev in obs_events:
            print(f"  • {ev}")
    else:
        print("  • None")
    print("Trend                   :")
    if obs_trend:
        for t_name, t_val in obs_trend.items():
            print(f"  • {t_name}: {t_val}")
    else:
        print("  • Stable")
        
    total_observations = sum(len(obs) for obs in observation_objects.values())
    print("Overall Statistics      :")
    print(f"  • Entities Received   : {total_entities}")
    print(f"  • Observations Created: {total_observations}")
    print("==================================================")
    
    # BEHAVIOR
    print_arrow()
    print("==================================================")
    print(f"{'Behavior':^50}")
    print("==================================================")
    print(f"Pipeline ID             : {representative_id}")
    beh_analysis = representative_behavior.get("behavior", {})
    print(f"Behavior Score          : {beh_analysis.get('behavior_score')} / 100")
    print(f"Severity                : {beh_analysis.get('severity')}")
    
    patterns = beh_analysis.get("patterns", [])
    print(f"Pattern                 : {', '.join(patterns) if patterns else 'None'}")
    
    beh_reasons = summarize_behavior_reason(beh_analysis, obs_events, obs_baseline, obs_trend)
    print("Reason                  :")
    for r in beh_reasons:
        print(f"  • {r}")
        
    total_behaviors = sum(len(beh) for beh in behavior_objects.values())
    beh_stats = {"CRITICAL": 0, "WARNING": 0, "NORMAL": 0}
    for ds, items in behavior_objects.items():
        for item in items:
            sev = item.get("behavior", {}).get("severity", "NORMAL").upper()
            beh_stats[sev] = beh_stats.get(sev, 0) + 1
            
    print("Overall Statistics      :")
    print(f"  • Behavior Objects    : {total_behaviors}")
    print(f"  • Critical            : {beh_stats.get('CRITICAL', 0)}")
    print(f"  • Warning             : {beh_stats.get('WARNING', 0)}")
    print(f"  • Normal              : {beh_stats.get('NORMAL', 0)}")
    print("==================================================")
    
    # RISK PREDICTION
    print_arrow()
    print("==================================================")
    print(f"{'Risk Prediction':^50}")
    print("==================================================")
    print(f"Pipeline ID             : {representative_id}")
    print(f"Risk Score              : {representative_risk.get('risk_score')} / 100")
    print(f"Risk Severity           : {representative_risk.get('risk_severity')}")
    print(f"Probability             : {representative_risk.get('risk_probability')}%")
    print(f"Category                : {representative_risk.get('risk_category')}")
    
    total_risks = sum(len(risk) for risk in risk_objects.values())
    risk_stats = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for ds, items in risk_objects.items():
        for item in items:
            sev = item.get("risk_severity", "LOW").upper()
            risk_stats[sev] = risk_stats.get(sev, 0) + 1
            
    print("Overall Statistics      :")
    print(f"  • Risk Objects        : {total_risks}")
    print(f"  • Critical            : {risk_stats.get('CRITICAL', 0)}")
    print(f"  • High                : {risk_stats.get('HIGH', 0)}")
    print(f"  • Medium              : {risk_stats.get('MEDIUM', 0)}")
    print(f"  • Low                 : {risk_stats.get('LOW', 0)}")
    print("==================================================")
    
    # INTEGRITY
    print_arrow()
    print("==================================================")
    print(f"{'Integrity':^50}")
    print("==================================================")
    print(f"Pipeline ID             : {representative_id}")
    print(f"Integrity Score         : {representative_integrity.get('integrity_score')} / 100")
    print(f"Integrity Status        : {representative_integrity.get('integrity_status')}")
    print(f"Trust Level             : {representative_integrity.get('output_trust_level')}")
    print(f"Primary Failure         : {representative_integrity.get('primary_failure_reason') or 'None'}")
    
    total_integrities = sum(len(integ) for integ in integrity_objects.values())
    integrity_stats = {"PASS": 0, "WARNING": 0, "FAIL": 0}
    for ds, items in integrity_objects.items():
        for item in items:
            status = item.get("integrity_status", "PASS").upper()
            if status == "FAILED":
                status = "FAIL"
            integrity_stats[status] = integrity_stats.get(status, 0) + 1
            
    print("Overall Statistics      :")
    print(f"  • Integrity Objects   : {total_integrities}")
    print(f"  • PASS                : {integrity_stats.get('PASS', 0)}")
    print(f"  • WARNING             : {integrity_stats.get('WARNING', 0)}")
    print(f"  • FAIL                : {integrity_stats.get('FAIL', 0)}")
    print("==================================================")
    
    # RECOMMENDATION
    print_arrow()
    print("==================================================")
    print(f"{'Recommendation':^50}")
    print("==================================================")
    print(f"Pipeline ID             : {representative_id}")
    print("Provider                : Ollama")
    print(f"Priority                : {representative_recommendation.get('priority')}")
    
    full_recommendation = representative_recommendation.get('recommendation', '')
    import textwrap
    wrapped_lines = textwrap.wrap(full_recommendation, width=90)
    
    print("Recommendation          :")
    print("------------------------------------------------------------------------------------------")
    for line in wrapped_lines:
        print(f"  {line}")
    print("------------------------------------------------------------------------------------------")
        
    print(f"Expected Impact         : {representative_recommendation.get('expected_impact')}")
    print(f"Recovery Time           : {representative_recommendation.get('estimated_recovery_time')}")
    print(f"Automation Possible     : {representative_recommendation.get('automation_possible')}")
    print(f"Human Approval Required : {representative_recommendation.get('human_approval_required')}")
    print(f"Recommendation Source   : {representative_recommendation.get('recommendation_source')}")
    print(f"Selected Model          : {representative_recommendation.get('selected_model') or 'None'}")
    print("==================================================")
    
    # RECOMMENDATION EVALUATION
    print_arrow()
    print("==================================================")
    print(f"{'Recommendation Evaluation':^50}")
    print("==================================================")
    print(f"Pipeline ID             : {representative_id}")
    
    from agents.recommendation.recommendation_context_builder import RecommendationContextBuilder
    from agents.recommendation.evaluation.golden_truth_repository import GoldenTruthRepository
    from agents.recommendation.evaluation.scenario_resolver import ScenarioResolver
    
    builder = RecommendationContextBuilder()
    rep_context = builder.build(
        representative_behavior,
        representative_risk,
        representative_integrity,
        representative_op_entity
    )
    repository = GoldenTruthRepository("data/evaluation/golden_truth_recommendations.json")
    resolver = ScenarioResolver()
    rep_scenarios = repository.get_all_scenarios()
    rep_scenario = resolver.resolve(rep_context, rep_scenarios)
    rep_scenario_name = rep_scenario.get("scenario_name") if (rep_scenario and rep_scenario.get("scenario_name")) else "Not Matched"
    
    print(f"Golden Truth Scenario   : {rep_scenario_name}")
    print("Evaluation Engine       : RAGAS")
    print("Metric                  : factual_correctness")
    
    rep_eval = representative_recommendation.get("evaluation", {})
    rep_status = rep_eval.get("status", "UNAVAILABLE")
    rep_metrics = rep_eval.get("metrics", {})
    rep_score = rep_metrics.get("factual_correctness")
    
    print(f"Status                  : {rep_status}")
    if rep_status == "COMPLETED" and rep_score is not None:
        print(f"Score                   : {rep_score}")
    else:
        print("Score                   : N/A")
        print("Evaluation unavailable.")
    print("==================================================")
    
    golden_truth_matched = 0
    golden_truth_unmatched = 0
    ragas_completed = 0
    ragas_unavailable = 0
    llm_generated = 0
    deterministic = 0
    
    for ds_name, recs in recommendation_objects.items():
        beh_map = {x.get("entity_id"): x for x in behavior_objects.get(ds_name, [])}
        risk_map = {x.get("entity_id"): x for x in risk_objects.get(ds_name, [])}
        integ_map = {x.get("entity_id"): x for x in integrity_objects.get(ds_name, [])}
        op_map = {x.get("entity_id"): x for x in operational_entities.get(ds_name, [])}
        
        for r in recs:
            eid = r.get("entity_id")
            
            if r.get("recommendation_source") == "llm":
                llm_generated += 1
            else:
                deterministic += 1
                
            eval_meta = r.get("evaluation", {})
            if eval_meta.get("status") == "COMPLETED":
                ragas_completed += 1
            else:
                ragas_unavailable += 1
                
            beh = beh_map.get(eid, {})
            risk = risk_map.get(eid, {})
            integ = integ_map.get(eid, {})
            op = op_map.get(eid, {})
            
            ctx = builder.build(beh, risk, integ, op)
            resolved = resolver.resolve(ctx, rep_scenarios)
            if resolved is not None:
                golden_truth_matched += 1
            else:
                golden_truth_unmatched += 1

    # FINAL EXECUTION SUMMARY
    print()
    print("==================================================")
    print(f"{'FINAL EXECUTION SUMMARY':^50}")
    print("==================================================")
    print("Pipeline Processing:")
    print(f"  Operational Entities        : {total_entities}")
    print(f"  Observation Objects         : {total_observations}")
    print(f"  Behavior Objects            : {total_behaviors}")
    print(f"  Risk Objects                : {total_risks}")
    print(f"  Integrity Objects           : {total_integrities}")
    print(f"  Recommendation Objects      : {total_integrities}")
    print("--------------------------------------------------")
    print("Recommendation Engine:")
    print(f"  LLM Generated               : {llm_generated}")
    print(f"  Deterministic               : {deterministic}")
    print("--------------------------------------------------")
    print("Evaluation:")
    print(f"  Golden Truth Matched        : {golden_truth_matched}")
    print(f"  Golden Truth Unmatched      : {golden_truth_unmatched}")
    print(f"  Representative Eval Status  : {rep_status}")
    print("--------------------------------------------------")
    print("Execution:")
    print(f"  Execution Time              : {execution_time} seconds")
    print(f"  Output File                 : {output_path}")
    print(f"  Execution Status            : SUCCESS")
    print("==================================================")
    



def print_workflow_banner():
    print("=" * 80)
    print("                ADAPTIVE INTELLIGENCE FABRIC (AIF)")
    print("=" * 80)


def main():
    import io
    import sys
    import contextlib

    start_time = time.perf_counter()

    print_workflow_banner()

    # Step 1: Capability Adapter
    print("Capability Adapter: Reading raw and business context sources...")
    
    connector = CSVConnector("config/sources.yaml")
    connector.connect()
    datasets = connector.read_all()
    connector.disconnect()

    raw_dataset_names = [
        "airflow",
        "kafka",
        "kubernetes",
        "azure_data_factory",
        "sap",
        "manufacturing",
        "iot"
    ]

    lookup_dataset_names = [
        "business_context",
        "historical_baselines",
        "incident_history",
        "pipeline_lineage",
        "recommendation_history"
    ]

    raw_datasets = {
        name: datasets[name]
        for name in raw_dataset_names
    }

    lookup_datasets = {
        name: datasets[name]
        for name in lookup_dataset_names
    }

    # Step 2: Tabular Parser
    parser = TabularParser()
    parsed_raw = parser.parse_all(raw_datasets)
    parsed_lookup = parser.parse_all(lookup_datasets)

    # Step 3: Mapping Engine
    mapper = MappingEngine("config/mapping.yaml")
    mapped_data = mapper.map_all(parsed_raw)

    # Step 4: Normalizer
    normalizer = Normalizer()
    normalized_data = normalizer.normalize_all(mapped_data)

    # Step 5: Context Enricher
    enricher = ContextEnricher(parsed_lookup)
    if hasattr(enricher, "health_check"):
        enricher.health_check()
    enriched_data = enricher.enrich_all(normalized_data)

    # Step 6: Validation Engine
    validator = ValidationEngine()
    validator.health_check()
    validation_results = validator.validate_all(enriched_data)

    # Step 7: Entity Builder
    entity_builder = OperationalEntityBuilder()
    operational_entities = entity_builder.build_all(validation_results)
    
    print("Capability Adapter Completed Successfully.")

    # Step 8: Observer Agent
    print("Observer Agent: Evaluating pipeline execution state and deviations...")
    observer = ObserverAgent()
    observer.health_check()
    observation_objects = observer.observe_all(operational_entities)
    print("Observer Agent Completed Successfully.")

    # Step 9: Behavior Agent
    print("Behavior Agent: Running baseline deviation and drift analysis...")
    behavior_agent = BehaviorAgent("config/behavior_rules.yaml")
    behavior_agent.health_check()
    behavior_objects = behavior_agent.analyze_all(observation_objects)
    print("Behavior Agent Completed Successfully.")

    # Step 10: Risk Prediction Agent
    print("Risk Prediction Agent: Evaluating business risks and probability...")
    risk_agent = RiskPredictionAgent("config/risk_rules.yaml")
    risk_agent.health_check()
    risk_objects = risk_agent.predict_all(behavior_objects)
    print("Risk Prediction Agent Completed Successfully.")

    # Step 11: Integrity Agent
    print("Integrity Agent: Validating schemas, lineage, and business rules...")
    integrity_agent = IntegrityAgent("config/integrity_rules.yaml")
    integrity_agent.health_check()
    integrity_objects = integrity_agent.evaluate_all(observation_objects)
    print("Integrity Agent Completed Successfully.")

    # Step 12: Recommendation Agent
    print("Recommendation Agent: Formulating optimal response strategies...")
    recommendation_agent = RecommendationAgent("config/recommendation_rules.yaml")
    recommendation_agent.health_check()

    # Pre-select representative pipeline dynamically from current results
    selected_pipeline, selection_reason = select_representative_pipeline_from_results(
        operational_entities,
        behavior_objects,
        risk_objects,
        integrity_objects,
        recommendation_agent
    )
    
    if selected_pipeline:
        representative_id = selected_pipeline["entity_id"]
        recommendation_agent.multi_llm_selection_agent.representative_entity_id = representative_id
        os.environ["LIMIT_EVALUATION"] = "true"
        os.environ["REPRESENTATIVE_PIPELINE_ID"] = representative_id
    else:
        representative_id = None

    # Suppress per-entity repetitive agent output and evaluation logs
    root_logger = logging.getLogger()
    old_log_level = root_logger.level
    root_logger.setLevel(logging.ERROR)
    
    logging.getLogger("langchain").setLevel(logging.ERROR)
    logging.getLogger("langchain_google_genai").setLevel(logging.ERROR)
    logging.getLogger("google").setLevel(logging.ERROR)
    logging.getLogger("ragas").setLevel(logging.ERROR)

    f_out = io.StringIO()
    f_err = io.StringIO()
    with contextlib.redirect_stdout(f_out), contextlib.redirect_stderr(f_err):
        try:
            recommendation_objects = recommendation_agent.generate_all(
                operational_entities,
                behavior_objects,
                risk_objects,
                integrity_objects
            )
        except Exception as e:
            logger.error(f"Error during recommendation generation: {e}")
            raise e
        finally:
            os.environ["LIMIT_EVALUATION"] = "false"
            
    print("Recommendation Agent Completed Successfully.")
    print()

    # Save to execution_output.json exactly as before
    execution_output = {
        "adapter_output": {
            "parsed_raw": parsed_raw,
            "parsed_lookup": parsed_lookup,
            "mapped_data": mapped_data,
            "normalized_data": normalized_data,
            "enriched_data": enriched_data,
            "validation_results": validation_results
        },
        "operational_entity": operational_entities,
        "observer_output": observation_objects,
        "behavior_output": behavior_objects,
        "risk_output": risk_objects,
        "integrity_output": integrity_objects,
        "recommendation_output": recommendation_objects
    }

    output_dir = "output"
    output_path = os.path.join(output_dir, "execution_output.json")
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(execution_output, file, indent=2, default=str)

    execution_time = round(time.perf_counter() - start_time, 2)

    # Print the clean step-by-step presentation for ONLY the representative pipeline
    if selected_pipeline:
        print_refined_execution_flow(
            selected_pipeline,
            selection_reason,
            parsed_raw,
            parsed_lookup,
            validation_results,
            operational_entities,
            observation_objects,
            behavior_objects,
            risk_objects,
            integrity_objects,
            recommendation_objects,
            recommendation_agent,
            execution_time,
            output_path
        )
    else:
        print("No representative pipeline could be selected.")

    return {
        "status": "SUCCESS",
        "execution_time": execution_time,
        "output_file": output_path,
        "adapter_summary": {
            "data_sources_loaded": len(datasets),
            "raw_datasets": len(raw_datasets),
            "lookup_datasets": len(lookup_datasets),
            "parsed_raw_records": sum(len(records) for records in parsed_raw.values()),
            "parsed_lookup_records": sum(len(records) for records in parsed_lookup.values()),
            "mapped_records": sum(len(records) for records in mapped_data.values()),
            "normalized_records": sum(len(records) for records in normalized_data.values()),
            "validated_records": sum(len(records) for records in validation_results.values()),
            "operational_entities": sum(len(v) for v in operational_entities.values())
        },
        "observer_summary": {
            "operational_entities": observer.total_entities,
            "observation_objects": observer.total_observations
        },
        "behavior_summary": {
            "observation_objects": behavior_agent.total_observations,
            "behavior_objects": behavior_agent.total_behaviors
        },
        "risk_summary": {
            "behavior_objects": risk_agent.total_behaviors,
            "risk_objects": risk_agent.total_risks
        },
        "integrity_summary": {
            "observation_objects": integrity_agent.total_observations,
            "integrity_objects": integrity_agent.total_integrities
        },
        "recommendation_summary": {
            "behavior_objects": recommendation_agent.total_behaviors,
            "recommendation_objects": recommendation_agent.total_recommendations,
            "generated_via_llm": recommendation_agent.total_generated_by_llm,
            "generated_via_deterministic": recommendation_agent.total_generated_by_deterministic
        }
    }

def derive_observation_status(events, baseline):

    if events or any(
        result.get("status") in {"WARNING", "CRITICAL"}
        for result in baseline.values()
    ):

        return "ATTENTION REQUIRED"

    return "NORMAL"


def summarize_baseline_comparison(baseline):

    if not baseline:

        return "No baseline comparison available"

    parts = []

    for metric_name, result in baseline.items():

        status = result.get("status", "UNKNOWN")
        deviation = result.get("deviation_percent")

        if deviation is None:

            parts.append(f"{metric_name.title()}={status}")

        else:

            parts.append(f"{metric_name.title()}={status} ({deviation}%)")

    return "; ".join(parts)


def derive_behavior_status(behavior_analysis, events, baseline, trend):

    if behavior_analysis.get("severity") in {"WARNING", "CRITICAL"}:

        return "ATTENTION REQUIRED"

    if behavior_analysis.get("patterns"):

        return "ATTENTION REQUIRED"

    if events or any(result.get("status") in {"WARNING", "CRITICAL"} for result in baseline.values()):

        return "ATTENTION REQUIRED"

    if any(value == "Increasing" for value in trend.values()):

        return "ATTENTION REQUIRED"

    return "NORMAL"


def derive_risk_status(risk_analysis):

    if risk_analysis.get("risk_severity") in {"HIGH", "CRITICAL"}:

        return "ATTENTION REQUIRED"

    return "NORMAL"


def summarize_observer_reason(events, baseline, trend):

    reasons = []

    for metric_name, result in baseline.items():

        deviation = result.get("deviation_percent")
        status = result.get("status")

        if status in {"WARNING", "CRITICAL"} and deviation is not None:

            reasons.append(f"{metric_name.title()} is {deviation}% above baseline")

    for metric_name, trend_value in trend.items():

        if trend_value == "Increasing":

            reasons.append(f"{metric_name.title()} is increasing")

    for event in events:

        reasons.append(f"{event} detected")

    if not reasons:

        reasons.append("No abnormal baseline deviation or events detected")

    return reasons


def summarize_behavior_reason(behavior_analysis, events, baseline, trend):

    reasons = []

    for metric_name, result in baseline.items():

        deviation = result.get("deviation_percent")
        status = result.get("status")

        if status in {"WARNING", "CRITICAL"} and deviation is not None:

            reasons.append(
                f"{metric_name.title()} exceeds historical baseline by {deviation}%"
            )

    for metric_name, trend_value in trend.items():

        if trend_value == "Increasing":

            reasons.append(f"{metric_name.title()} trend is increasing")

    for event in events:

        reasons.append(f"{event} detected")

    for pattern in behavior_analysis.get("patterns", []):

        reasons.append(pattern)

    if not reasons:

        reasons.append("Behavior remains within expected thresholds")

    return reasons


def format_deviation_score(representative_flow):

    baseline = representative_flow.get("observation_baseline", {})

    if not baseline:

        return "None"

    deviations = []

    for result in baseline.values():

        deviation = result.get("deviation_percent")

        if deviation is not None:

            deviations.append(abs(deviation))

    if not deviations:

        return "None"

    return f"{max(deviations)}%"


def format_drift_status(representative_flow):

    drift = representative_flow.get("behavior_analysis", {}).get("drift", {})

    if not drift:

        return "None"

    if any(result.get("is_drifting") for result in drift.values()):

        return "DETECTED"

    return "CLEAR"


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    main()