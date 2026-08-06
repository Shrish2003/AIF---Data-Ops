from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class PipelineDetailsResponse(BaseModel):
    operational_entity: Optional[Dict[str, Any]] = None
    observation_object: Optional[Dict[str, Any]] = None
    behavior_object: Optional[Dict[str, Any]] = None
    risk_object: Optional[Dict[str, Any]] = None
    integrity_object: Optional[Dict[str, Any]] = None
    recommendation_object: Optional[Dict[str, Any]] = None

class TimelineEvent(BaseModel):
    timestamp: str
    status: str
    metrics: Dict[str, Any]

class TimelineResponse(BaseModel):
    pipeline_id: str
    timeline: List[TimelineEvent]

class LineageRelation(BaseModel):
    entity_id: str
    source_system: str
    depends_on: str
    downstream: str
    dependency_type: str

class DependencyResponse(BaseModel):
    pipeline_id: str
    upstream_pipelines: List[str]
    downstream_pipelines: List[str]
    pipeline_lineage: List[LineageRelation]
    dependency_count: int

class RecommendationResponse(BaseModel):
    pipeline_id: str
    priority: Optional[str] = None
    recommendation: Optional[str] = None
    expected_impact: Optional[str] = None
    recovery_time: Optional[str] = None
    automation_possible: Optional[bool] = None
    human_approval_required: Optional[bool] = None
    confidence: Optional[float] = None
    generated_by: Optional[str] = None
    model: Optional[str] = None
    recommendation_source: Optional[str] = None

class CopilotChatResponse(BaseModel):
    reply: str
    rootCause: str
    affectedServices: List[str]
    businessImpact: str
    suggestedActions: List[str]
    confidence: float
    followUpQuestions: List[str]
    agentSources: List[str]

class AgentHealthDetail(BaseModel):
    status: str
    version: str
    health: str

class AgentsResponse(BaseModel):
    agents: Dict[str, AgentHealthDetail]

class DashboardSummaryResponse(BaseModel):
    execution_summary: Dict[str, Any]
    capability_adapter_summary: Dict[str, Any]
    observer_summary: Dict[str, Any]
    behavior_summary: Dict[str, Any]
    risk_summary: Dict[str, Any]
    integrity_summary: Dict[str, Any]
    recommendation_summary: Dict[str, Any]
    top_critical_pipelines: List[Dict[str, Any]]
    agent_health: Dict[str, AgentHealthDetail]
    representative_pipeline: PipelineDetailsResponse
    latest_recommendation: Optional[Dict[str, Any]] = None
    execution_time: float
    status: str

# ==========================================================
# Unified Dashboard API Contract Schemas
# ==========================================================

class UnifiedSummary(BaseModel):
    overallHealth: int
    overallStatus: str
    riskLevel: str
    activeAnomalies: int
    criticalAnomalies: int
    executionStatus: Optional[str] = None
    lastExecutionTime: Optional[str] = None

class UnifiedHealthNode(BaseModel):
    id: str
    label: str
    score: int
    status: str
    platform: Optional[str] = None
    lastExecution: Optional[str] = None
    pipelineId: Optional[str] = None
    pipelineName: Optional[str] = None
    healthScore: Optional[int] = None

class UnifiedHealth(BaseModel):
    nodes: List[UnifiedHealthNode]

class UnifiedAnomaly(BaseModel):
    id: str
    sev: str
    severity: Optional[str] = None
    time: str
    service: str
    pipeline: Optional[str] = None
    signal: str
    agent: str

class UnifiedAgent(BaseModel):
    id: str
    displayName: Optional[str] = None
    status: str
    signals: int
    signalsProcessed: Optional[int] = None
    health: Optional[str] = None

class UnifiedRiskSummary(BaseModel):
    servicesAtRisk: int
    criticalPipelines: int
    cascadeProbability: int
    monitoringCoverage: int
    agentSignals: int

class CopilotContext(BaseModel):
    summary: Dict[str, Any]
    anomalies: List[Dict[str, Any]]
    health: Dict[str, Any]
    agents: List[Dict[str, Any]]

class UnifiedDashboardResponse(BaseModel):
    useCaseId: str
    useCaseName: str
    timestamp: str
    summary: UnifiedSummary
    health: UnifiedHealth
    anomalies: List[UnifiedAnomaly]
    agents: List[UnifiedAgent]
    riskSummary: Optional[UnifiedRiskSummary] = None
    copilot: Optional[CopilotContext] = None

