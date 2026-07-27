import pytest
from api.services.dashboard_service import DashboardService
from api.services.copilot_service import CopilotService

def test_unified_dashboard_format():
    response = DashboardService.get_dashboard()
    
    # Verify top-level structure
    assert response["useCaseId"] == "data-ops"
    assert response["useCaseName"] == "Data Pipeline Operations Intelligence"
    assert "timestamp" in response
    
    # Verify summary section
    summary = response["summary"]
    assert isinstance(summary["overallHealth"], int)
    assert summary["overallStatus"] in ["healthy", "degraded", "critical"]
    assert summary["riskLevel"] == "medium"
    assert isinstance(summary["activeAnomalies"], int)
    assert isinstance(summary["criticalAnomalies"], int)
    assert summary["executionStatus"] == "SUCCESS"
    assert "lastExecutionTime" in summary
    
    # Verify health section
    health = response["health"]
    assert "nodes" in health
    assert len(health["nodes"]) >= 8
    for node in health["nodes"]:
        assert "id" in node
        assert "label" in node
        assert isinstance(node["score"], int)
        assert node["status"] in ["healthy", "degraded", "critical"]
        assert node["platform"] in ["Kafka", "Airflow", "Kubernetes", "Azure Data Factory", "SAP ERP"]
        assert "lastExecution" in node
        assert "pipelineId" in node
        assert "pipelineName" in node
        assert isinstance(node["healthScore"], int)
        
    # Verify anomalies section
    anomalies = response["anomalies"]
    assert len(anomalies) > 0
    for anom in anomalies:
        assert "id" in anom
        assert "sev" in anom
        assert "severity" in anom
        assert "time" in anom
        assert "service" in anom
        assert "pipeline" in anom
        assert "signal" in anom
        assert "agent" in anom

    # Verify agents section
    agents = response["agents"]
    assert len(agents) == 6
    for agent in agents:
        assert "id" in agent
        assert "displayName" in agent
        assert "status" in agent
        assert isinstance(agent["signals"], int)
        assert isinstance(agent["signalsProcessed"], int)
        assert "health" in agent

    # Verify riskSummary section
    risk_summary = response["riskSummary"]
    assert isinstance(risk_summary["servicesAtRisk"], int)
    assert isinstance(risk_summary["cascadeProbability"], int)
    assert isinstance(risk_summary["monitoringCoverage"], int)
    assert isinstance(risk_summary["agentSignals"], int)
    assert "criticalPipelines" in risk_summary

    # Verify copilot context section
    copilot = response["copilot"]
    assert "summary" in copilot
    assert "anomalies" in copilot
    assert "health" in copilot
    assert "agents" in copilot


def test_copilot_chat_format():
    # Chat with specific BRO0001 query
    resp_bro = CopilotService.chat("tell me about BRO0001")
    assert "reply" in resp_bro
    assert "suggestedActions" in resp_bro
    assert resp_bro["confidence"] == "high"
    assert "followUpQuestions" in resp_bro
    
    # Chat with generic query
    resp_generic = CopilotService.chat("is the pipeline stable?")
    assert "reply" in resp_generic
    assert "suggestedActions" in resp_generic
    assert resp_generic["confidence"] == "medium"
    assert "followUpQuestions" in resp_generic
