import json
import logging
from typing import Dict, Any, Optional

from api.services.dashboard_service import DashboardService
from api.services.pipeline_details_service import PipelineDetailsService

logger = logging.getLogger(__name__)

class OperationalIntelligenceContextBuilder:

    @staticmethod
    def build_context(pipeline_id: Optional[str], use_case_id: str, domain_id: str) -> Dict[str, Any]:
        """
        Consolidates dashboard data and pipeline details (including AIF agent outputs)
        into one standardized operational intelligence context object.
        """
        logger.info(f"Building operational intelligence context for pipeline_id={pipeline_id}, use_case={use_case_id}, domain={domain_id}")
        
        # 1. Retrieve latest dashboard state
        dashboard_data = {}
        try:
            dashboard_data = DashboardService.get_dashboard()
        except Exception as e:
            logger.error(f"ContextBuilder failed to fetch dashboard state: {e}")

        # 2. Retrieve latest pipeline details if pipeline_id is provided
        pipeline_data = {}
        if pipeline_id:
            try:
                pipeline_data = PipelineDetailsService.get_pipeline(pipeline_id)
            except Exception as e:
                logger.error(f"ContextBuilder failed to fetch pipeline details for {pipeline_id}: {e}")

        # Extract agent intelligence outputs safely from the pipeline details
        observer_output = pipeline_data.get("observation_object")
        behavior_output = pipeline_data.get("behavior_object")
        risk_output = pipeline_data.get("risk_object")
        integrity_output = pipeline_data.get("integrity_object")
        recommendation_output = pipeline_data.get("recommendation_object")

        # Conjoin all sources into a single structured operational context object
        context = {
            "domain": {
                "useCaseId": use_case_id,
                "domainId": domain_id,
                "activePipelineId": pipeline_id
            },
            "dashboardSummary": {
                "overallHealth": dashboard_data.get("summary", {}).get("overallHealth"),
                "overallStatus": dashboard_data.get("summary", {}).get("overallStatus"),
                "riskLevel": dashboard_data.get("summary", {}).get("riskLevel"),
                "activeAnomaliesCount": dashboard_data.get("summary", {}).get("activeAnomalies"),
                "criticalAnomaliesCount": dashboard_data.get("summary", {}).get("criticalAnomalies"),
                "riskSummary": dashboard_data.get("riskSummary", {})
            },
            "activeAnomalies": dashboard_data.get("anomalies", []),
            "platformNodeHealth": dashboard_data.get("health", {}).get("nodes", []),
            "agentStatuses": dashboard_data.get("agents", []),
            
            # Agent specific intelligence
            "pipelineDetails": {
                "pipelineId": pipeline_id,
                "operationalEntity": pipeline_data.get("operational_entity"),
            },
            "agentIntelligence": {
                "observerAgent": observer_output,
                "behaviorAgent": behavior_output,
                "riskPredictionAgent": risk_output,
                "integrityAgent": integrity_output,
                "recommendationAgent": recommendation_output
            }
        }
        
        return context
