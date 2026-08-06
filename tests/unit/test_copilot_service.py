import pytest
from api.schemas.request import CopilotQuery
from api.services.context_builder import OperationalIntelligenceContextBuilder
from api.services.copilot_service import CopilotService

def test_context_builder_dashboard():
    # Test context builder with dashboard context (no pipeline ID)
    ctx = OperationalIntelligenceContextBuilder.build_context(
        pipeline_id=None,
        use_case_id="data-ops",
        domain_id="default"
    )
    assert "domain" in ctx
    assert ctx["domain"]["useCaseId"] == "data-ops"
    assert ctx["domain"]["activePipelineId"] is None
    
    assert "dashboardSummary" in ctx
    assert "overallHealth" in ctx["dashboardSummary"]
    assert "activeAnomalies" in ctx
    assert "agentStatuses" in ctx
    
    # When no pipeline ID is provided, agentIntelligence outputs should be None
    intel = ctx.get("agentIntelligence", {})
    assert intel.get("observerAgent") is None
    assert intel.get("behaviorAgent") is None

def test_context_builder_pipeline():
    # Test context builder with a valid pipeline ID
    ctx = OperationalIntelligenceContextBuilder.build_context(
        pipeline_id="BRO0001",
        use_case_id="data-ops",
        domain_id="default"
    )
    assert ctx["domain"]["activePipelineId"] == "BRO0001"
    assert "pipelineDetails" in ctx
    assert ctx["pipelineDetails"]["pipelineId"] == "BRO0001"
    
    intel = ctx.get("agentIntelligence", {})
    # Verify agent outputs are present
    assert intel.get("observerAgent") is not None
    assert intel.get("behaviorAgent") is not None
    assert intel.get("riskPredictionAgent") is not None
    assert intel.get("integrityAgent") is not None
    assert intel.get("recommendationAgent") is not None

def test_copilot_chat_end_to_end():
    # Test chat end-to-end payload handling and structured output format
    payload = CopilotQuery(
        useCaseId="data-ops",
        domainId="default",
        question="What should I investigate first for BRO0001?",
        context=None
    )
    response = CopilotService.chat(payload)
    
    # Assert clean unified schema fields are present
    assert "reply" in response
    assert "rootCause" in response
    assert "affectedServices" in response
    assert "businessImpact" in response
    assert "suggestedActions" in response
    assert "confidence" in response
    assert "followUpQuestions" in response
    assert "agentSources" in response
    
    assert isinstance(response["confidence"], float)
    assert response["confidence"] > 0.0
    assert len(response["agentSources"]) > 0

def test_copilot_session_memory():
    # Test conversation memory tracking across turns
    session_key = "data-ops:default"
    
    # Reset session memory if exists
    if session_key in CopilotService._sessions:
        del CopilotService._sessions[session_key]
        
    # Turn 1: Mention BRO0001 explicitly
    payload_1 = CopilotQuery(
        useCaseId="data-ops",
        domainId="default",
        question="Tell me about pipeline BRO0001 anomalies",
        context=None
    )
    response_1 = CopilotService.chat(payload_1)
    
    # Verify current pipeline is saved in session
    session = CopilotService._sessions.get(session_key)
    assert session is not None
    assert session["current_pipeline"] == "BRO0001"
    
    # Turn 2: Follow-up query without pipeline ID
    payload_2 = CopilotQuery(
        useCaseId="data-ops",
        domainId="default",
        question="What services are affected?",
        context=None
    )
    response_2 = CopilotService.chat(payload_2)
    
    # Verify that the response mapped to BRO0001 and included agent sources from it
    assert response_2["confidence"] > 0.0
    assert "Behavior Agent" in response_2["agentSources"]

def test_intent_classification():
    # Test Greeting intent
    assert CopilotService.classify_intent("Hi") == "greeting"
    assert CopilotService.classify_intent("Hello!") == "greeting"
    assert CopilotService.classify_intent("Good morning") == "greeting"
    
    # Test Small Talk / Thanks / Goodbye
    assert CopilotService.classify_intent("Thank you") == "small_talk"
    assert CopilotService.classify_intent("thanks a lot") == "small_talk"
    assert CopilotService.classify_intent("bye-bye") == "small_talk"
    
    # Test Identity
    assert CopilotService.classify_intent("Who are you?") == "identity"
    assert CopilotService.classify_intent("What can you do?") == "identity"
    assert CopilotService.classify_intent("explain yourself") == "identity"
    
    # Test Help
    assert CopilotService.classify_intent("help") == "help"
    assert CopilotService.classify_intent("what can I ask?") == "help"
    
    # Test Unsupported
    assert CopilotService.classify_intent("Capital of France?") == "unsupported"
    assert CopilotService.classify_intent("Write Python code for sorting") == "unsupported"
    
    # Test Operational
    assert CopilotService.classify_intent("Investigate BRO0001") == "operational"
    assert CopilotService.classify_intent("Why is risk score high?") == "operational"
    assert CopilotService.classify_intent("Show anomalies") == "operational"

def test_conversational_direct_greetings():
    payload = CopilotQuery(
        useCaseId="data-ops",
        domainId="default",
        question="Hello, how's it going?",
        context=None
    )
    response = CopilotService.chat(payload)
    
    assert "hello" in response["reply"].lower()
    assert response["rootCause"] == "N/A (Greeting)"
    assert response["confidence"] == 1.0
    assert len(response["suggestedActions"]) > 0
    assert len(response["agentSources"]) == 0
