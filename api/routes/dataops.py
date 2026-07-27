from fastapi import APIRouter, Path, Body
from typing import Dict, Any

from api.schemas.request import CopilotQuery
from api.schemas.response import (
    DashboardSummaryResponse,
    PipelineDetailsResponse,
    TimelineResponse,
    DependencyResponse,
    RecommendationResponse,
    CopilotChatResponse,
    AgentsResponse,
    UnifiedDashboardResponse
)
from api.services.pipeline_service import PipelineService
from api.services.dashboard_service import DashboardService
from api.services.pipeline_details_service import PipelineDetailsService
from api.services.recommendation_service import RecommendationService
from api.services.copilot_service import CopilotService
from api.services.agent_service import AgentService

router = APIRouter(
    prefix="/api/v1/dataops",
    tags=["Data Pipeline Operations Intelligence"]
)


@router.post("/execute", summary="Execute complete AIF pipeline")
def execute_pipeline() -> Dict[str, Any]:
    """
    Execute complete AIF pipeline workflow.
    """
    return PipelineService.execute()


@router.get("/dashboard", response_model=UnifiedDashboardResponse, summary="Get unified dashboard data")
def get_dashboard():
    """
    Retrieve and aggregate summaries for the unified dashboard.
    """
    return DashboardService.get_dashboard()


@router.get("/pipelines/{pipelineId}", response_model=PipelineDetailsResponse, summary="Get pipeline details")
def get_pipeline(pipeline_id: str = Path(..., description="The ID of the pipeline (e.g. BRO0001)", alias="pipelineId")):
    """
    Retrieve all AIF layers (operational, observation, behavior, risk, integrity, recommendation) for a pipeline.
    """
    return PipelineDetailsService.get_pipeline(pipeline_id)


@router.get("/pipelines/{pipelineId}/timeline", response_model=TimelineResponse, summary="Get pipeline execution timeline")
def get_pipeline_timeline(pipeline_id: str = Path(..., description="The ID of the pipeline (e.g. BRO0001)", alias="pipelineId")):
    """
    Retrieve or construct execution history timestamps for the selected pipeline.
    """
    return PipelineDetailsService.get_pipeline_timeline(pipeline_id)


@router.get("/pipelines/{pipelineId}/dependencies", response_model=DependencyResponse, summary="Get pipeline dependencies")
def get_pipeline_dependencies(pipeline_id: str = Path(..., description="The ID of the pipeline (e.g. BRO0001)", alias="pipelineId")):
    """
    Retrieve lineage, upstream, and downstream dependencies using historical context datasets.
    """
    return PipelineDetailsService.get_pipeline_dependencies(pipeline_id)


@router.get("/recommendations/{pipelineId}", response_model=RecommendationResponse, summary="Get pipeline recommendation")
def get_recommendation(pipeline_id: str = Path(..., description="The ID of the pipeline (e.g. BRO0001)", alias="pipelineId")):
    """
    Retrieve recommendation assessment metadata for the selected pipeline.
    """
    return RecommendationService.get_recommendation(pipeline_id)


@router.post("/copilot/chat", response_model=CopilotChatResponse, summary="Chat with Operations Copilot")
def copilot_chat(payload: CopilotQuery = Body(...)):
    """
    Interface with the Operations Copilot backend to ask questions about pipeline executions.
    """
    return CopilotService.chat(payload.question)



@router.get("/agents", response_model=AgentsResponse, summary="Get pipeline agent status")
def get_agents():
    """
    Retrieve operational health, status, and versions of the AIF Agents.
    """
    return AgentService.get_agents()
