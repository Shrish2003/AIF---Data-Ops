import logging
import re
import json
import time
import traceback
import requests
from typing import Dict, Any, List, Optional

from api.schemas.request import CopilotQuery
from api.services.context_builder import OperationalIntelligenceContextBuilder
from agents.multi_llm_selection.llm_config import LLMConfig

logger = logging.getLogger(__name__)

class CopilotService:
    # Local session context dictionary to track current_domain, current_pipeline, current_incident, and conversation history
    # Key format: "useCaseId:domainId"
    _sessions: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def classify_intent(cls, question: str, active_pipeline: Optional[str] = None) -> str:
        """
        Classifies the user query into Greeting, Small Talk, Identity, Help, Operational, or Unsupported.
        """
        q = question.lower().strip()
        # Clean punctuation but keep hyphens and alphanumeric characters
        q_clean = re.sub(r'[^\w\s-]', '', q)
        
        # 1. Operational Check
        operational_keywords = [
            "risk", "anomaly", "anomalies", "affect", "affected", "services", "pipeline", "pipelines",
            "health", "lag", "throughput", "fail", "failure", "failed", "recommendation",
            "recommendations", "investigate", "incident", "incidents", "integrity", "validation",
            "status", "stable", "deviate", "deviation", "drift", "observer", "behavior",
            "check", "remediation", "action", "actions", "impact", "blast", "downstream", "upstream",
            "triage", "issue", "issues", "error", "errors", "symptom", "symptoms", "troubleshoot",
            "troubleshooting", "diagnostic", "diagnose", "run", "execution", "history", "lineage",
            "dependency", "dependencies", "latency", "congest", "congestion", "metrics", "alert", "alerts"
        ]
        
        has_pipeline_id = re.search(r'\b([A-Za-z]+[-_]?\d+)\b', question) is not None
        
        has_op_keyword_or_phrase = any(w in q_clean.split() for w in operational_keywords) or any(
            phrase in q for phrase in [
                "how is the", "status of", "what is the", "tell me about", "why is", "is the",
                "what should i", "explain the", "show active", "show recommendations", "explain overall health"
            ]
        )
        
        if has_pipeline_id or has_op_keyword_or_phrase:
            return "operational"
            
        # 2. Help Request Check
        help_keywords = ["help", "commands", "what can i ask", "show commands", "example questions", "examples"]
        if any(q_clean == kw or q_clean == f"show {kw}" or kw in q for kw in help_keywords):
            return "help"
            
        # 3. Identity Questions Check
        identity_keywords = [
            "who are you", "what can you do", "how do you work", "explain yourself",
            "tell me about yourself", "your name", "who is this", "what is your purpose"
        ]
        if any(phrase in q for phrase in identity_keywords) or q_clean in ["who are you", "what are you", "explain yourself"]:
            return "identity"
            
        # 4. Small Talk Check
        small_talk_keywords = [
            "how are you", "thanks", "thank you", "thx", "thank", "good job", "great job",
            "awesome", "perfect", "cool", "bye", "goodbye", "see you", "see ya", "exit",
            "quit", "farewell", "talk to you later", "bye-bye"
        ]
        if any(phrase in q_clean for phrase in small_talk_keywords) or any(w in q_clean.split() for w in ["thanks", "thank", "bye", "goodbye", "byebye"]):
            return "small_talk"
            
        # 5. Greeting Check
        greeting_keywords = ["hi", "hello", "hey", "greetings", "good morning", "good evening", "good afternoon", "yo"]
        if any(q_clean == kw or q_clean.startswith(kw + " ") or kw in q_clean.split() for kw in greeting_keywords):
            return "greeting"
            
        # 6. Session Follow-up Check
        if active_pipeline and len(q_clean.split()) > 1:
            return "operational"
            
        return "unsupported"

    @classmethod
    def _calculate_confidence(cls, agent_intel: Dict[str, Any]) -> float:
        """
        Calculate confidence score deterministically based on agent outputs:
        - Risk Prediction: 40%
        - Behavior Analysis: 30%
        - Integrity Validation: 20%
        - Recommendation Confidence: 10%
        """
        weights = {
            "risk": 0.40,
            "behavior": 0.30,
            "integrity": 0.20,
            "recommendation": 0.10
        }
        
        scores = {}
        
        risk = agent_intel.get("riskPredictionAgent")
        if risk and isinstance(risk, dict):
            scores["risk"] = risk.get("prediction_confidence") or (1.0 - (risk.get("risk_score", 0) / 100.0))
            
        beh = agent_intel.get("behaviorAgent")
        if beh and isinstance(beh, dict):
            scores["behavior"] = beh.get("behavior", {}).get("confidence") or beh.get("confidence") or 0.8
            
        integ = agent_intel.get("integrityAgent")
        if integ and isinstance(integ, dict):
            trust_level = integ.get("output_trust_level")
            if trust_level == "HIGH":
                scores["integrity"] = 0.95
            elif trust_level == "MEDIUM":
                scores["integrity"] = 0.80
            elif trust_level == "LOW":
                scores["integrity"] = 0.50
            else:
                scores["integrity"] = (integ.get("integrity_score", 100.0) / 100.0)
                
        rec = agent_intel.get("recommendationAgent")
        if rec and isinstance(rec, dict):
            scores["recommendation"] = rec.get("confidence") or 0.8
            
        total_weight = sum(weights[k] for k in scores)
        if total_weight > 0:
            weighted_score = sum(scores[k] * weights[k] for k in scores) / total_weight
            return round(weighted_score, 2)
        
        return 0.85

    @classmethod
    def chat(cls, payload) -> dict:
        if isinstance(payload, str):
            from api.schemas.request import CopilotQuery
            payload = CopilotQuery(
                useCaseId="data-ops",
                domainId="default",
                question=payload,
                context=None
            )
            
        start_time = time.time()
        question = payload.question
        payload_context = payload.context
        
        logger.info(f"Incoming Query: {question}")
        
        # 1. Retrieve or initialize the session
        session_key = f"{payload.useCaseId or 'default'}:{payload.domainId or 'default'}"
        session = cls._sessions.setdefault(session_key, {
            "current_domain": payload.domainId,
            "current_use_case": payload.useCaseId,
            "current_pipeline": None,
            "current_incident": payload.incidentId if hasattr(payload, 'incidentId') else None,
            "history": []
        })
        
        history_list = session["history"]
        if payload_context and isinstance(payload_context, dict):
            for key in ["history", "chat_history", "messages"]:
                if key in payload_context and isinstance(payload_context[key], list):
                    history_list = payload_context[key]
                    break
        
        logger.info(f"Conversation Memory before query: {json.dumps(history_list)}")
        
        # Update session metadata if provided in payload
        if payload.domainId:
            session["current_domain"] = payload.domainId
        if payload.useCaseId:
            session["current_use_case"] = payload.useCaseId
        if hasattr(payload, 'incidentId') and payload.incidentId:
            session["current_incident"] = payload.incidentId
            
        # 2. Run Intent Classification
        active_pipeline = session.get("current_pipeline")
        intent = cls.classify_intent(question, active_pipeline)
        logger.info(f"Classified Intent: {intent}")
        
        # Handle non-operational intents directly
        if intent != "operational":
            logger.info("Non-operational query detected. Generating structured response directly.")
            response_time = time.time() - start_time
            
            if intent == "greeting":
                reply = (
                    "Hello! I'm the Operations AI Copilot for Adaptive Intelligence Fabric.\n\n"
                    "I can help you investigate pipeline incidents, explain operational risks, analyze anomalies, "
                    "identify affected services, and provide AI-powered recommendations.\n\n"
                    "How can I assist you today?"
                )
                suggested = ["What should I investigate first?", "Show active anomalies", "Explain Overall Health"]
                follow_up = ["Would you like to check the overall system health?"]
                root_cause = "N/A (Greeting)"
                
            elif intent == "small_talk":
                # Check for thank you vs goodbye vs general small talk
                is_thank_you = any(w in q_clean for w in ["thank", "thanks", "thx", "good job", "great job", "awesome", "perfect", "cool"])
                is_goodbye = any(w in q_clean for w in ["bye", "goodbye", "see you", "see ya", "exit", "quit", "farewell"])
                
                if is_thank_you:
                    reply = "You're welcome! If you need help investigating pipeline incidents, understanding operational risks, or analyzing system health, I'm here to help."
                    suggested = ["Show active anomalies", "Explain Overall Health", "Show recommendations"]
                    follow_up = ["Is there anything else I can check for you?"]
                elif is_goodbye:
                    reply = "Goodbye! Feel free to return anytime if you need assistance with Data Pipeline Operations Intelligence. Have a great day!"
                    suggested = []
                    follow_up = []
                else: # general small talk (e.g. How are you?)
                    reply = "I am doing well, thank you! As the Operations AI Copilot, I am here to help you analyze pipeline logs, investigate anomalies, or answer system health queries. How can I assist you today?"
                    suggested = ["What should I investigate first?", "Show active anomalies"]
                    follow_up = ["Would you like to analyze a specific pipeline ID?"]
                root_cause = "N/A (Small Talk)"
                
            elif intent == "identity":
                reply = (
                    "I am the Operations AI Copilot for the Adaptive Intelligence Fabric (AIF).\n\n"
                    "I function as a reasoning and natural language explanation layer over the operational intelligence generated "
                    "by AIF agents. I do not detect anomalies or calculate risks myself; instead, I analyze and summarize "
                    "the outputs of our specialized agent pipeline:\n"
                    "- **Capability Adapter**: Standardizes incoming data.\n"
                    "- **Observer Agent**: Collects observations and health data.\n"
                    "- **Behavior Agent**: Identifies deviations and metric anomalies.\n"
                    "- **Risk Prediction Agent**: Models cascading risks and probabilities.\n"
                    "- **Integrity Agent**: Validates data quality rules.\n"
                    "- **Recommendation Agent**: Formulates base troubleshooting actions."
                )
                suggested = ["What should I investigate first?", "Show active anomalies"]
                follow_up = ["Would you like to analyze a specific pipeline ID?"]
                root_cause = "N/A (Identity)"
                
            elif intent == "help":
                reply = (
                    "I am here to assist you with Data Pipeline Operations Intelligence. Here are some examples of what you can ask me:\n\n"
                    "- **Triage**: *'What should I investigate first?'* or *'Show active anomalies.'*\n"
                    "- **System Health**: *'Explain Overall Health'* or *'Explain the current risk.'*\n"
                    "- **RCA & Impact**: *'Which services are affected?'* or *'Show recommendations.'*"
                )
                suggested = ["What should I investigate first?", "Explain Overall Health", "Show active anomalies"]
                follow_up = ["What area would you like help with?"]
                root_cause = "N/A (Help)"
                
            else:  # unsupported
                reply = (
                    "I am specialized in Adaptive Intelligence Fabric and Data Pipeline Operations Intelligence. "
                    "I can assist with pipeline health, operational risks, anomalies, recommendations, investigations, "
                    "and AI-powered operational insights."
                )
                suggested = ["What should I investigate first?", "Show active anomalies"]
                follow_up = ["Would you like me to show the active anomalies instead?"]
                root_cause = "N/A (Unsupported)"
                
            response_dict = {
                "reply": reply,
                "rootCause": root_cause,
                "affectedServices": [],
                "businessImpact": "None",
                "suggestedActions": suggested,
                "confidence": 1.0,
                "followUpQuestions": follow_up,
                "agentSources": []
            }
            
            logger.info(f"API Response: {json.dumps(response_dict, indent=2)}")
            logger.info(f"Response Time: {response_time:.4f}s")
            
            # Update history with simple conversational turn
            new_history = list(history_list)
            new_history.append({"role": "user", "content": question})
            new_history.append({"role": "assistant", "content": reply})
            session["history"] = new_history[-10:]
            
            return response_dict

        # 3. Extract pipeline ID from the question using regex
        pipeline_id = None
        pipeline_id_match = re.search(r'\b([A-Za-z]+[-_]?\d+)\b', question)
        if pipeline_id_match:
            pipeline_id = pipeline_id_match.group(1).upper()
            session["current_pipeline"] = pipeline_id
            logger.info(f"Parsed Pipeline ID from query: {pipeline_id}")
        else:
            pipeline_id = session.get("current_pipeline")
            if pipeline_id:
                logger.info(f"No pipeline ID in query. Retrieved from Session Memory: {pipeline_id}")
                
        # 4. Retrieve normalized intelligence context from Context Builder
        context = {}
        try:
            context = OperationalIntelligenceContextBuilder.build_context(
                pipeline_id=pipeline_id,
                use_case_id=payload.useCaseId or "default",
                domain_id=payload.domainId or "default"
            )
        except Exception as e:
            logger.error(f"Failed to build context: {e}")
            logger.error(traceback.format_exc())
            
        # Log retrieved/constructed operational intelligence context
        logger.info(f"Retrieved Dashboard Context: {json.dumps(context.get('dashboardSummary'), indent=2)}")
        logger.info(f"Retrieved Pipeline Context: {json.dumps(context.get('pipelineDetails'), indent=2)}")
        logger.info(f"Retrieved Agent Outputs: {json.dumps(context.get('agentIntelligence'), indent=2)}")
        logger.info(f"Constructed Operational Intelligence Context: {json.dumps(context, indent=2)}")
        
        # 5. Determine calculated confidence score and agent sources
        agent_intel = context.get("agentIntelligence", {})
        confidence_val = cls._calculate_confidence(agent_intel)
        
        # Populate agent sources based on available agent outputs in context
        agent_sources = []
        agent_mapping = {
            "observerAgent": "Observer Agent",
            "behaviorAgent": "Behavior Agent",
            "riskPredictionAgent": "Risk Prediction Agent",
            "integrityAgent": "Integrity Agent",
            "recommendationAgent": "Recommendation Agent"
        }
        for k, name in agent_mapping.items():
            if agent_intel.get(k) is not None:
                agent_sources.append(name)
                
        # 6. Format prompt
        # Strict enterprise system prompt instruction
        system_instructions = (
            "You are the Operations AI Copilot for the Adaptive Intelligence Fabric (AIF).\n"
            "You behave as a natural enterprise AI assistant similar to Microsoft Copilot.\n"
            "You answer ONLY using the provided operational intelligence context.\n\n"
            "CRITICAL INSTRUCTIONS:\n"
            "1. Answer ONLY using the supplied Dashboard, Pipeline, and Agent Intelligence context.\n"
            "2. Do NOT fabricate, invent, or guess details. If information is unavailable or not mentioned in the context to answer the question, explicitly state that it is unavailable.\n"
            "3. Keep responses concise, professional, and enterprise-focused.\n"
            "4. ENTERPRISE RESPONSE STYLE: Never expose JSON structures, internal Python object names, dictionaries, raw API responses, variable names, or object names (e.g. do NOT use terms like 'observation_object', 'behavior_object', 'recommendationAgent', 'response JSON', etc.).\n"
            "5. NATURAL LANGUAGE: Responses must be conversational. Avoid phrases like 'The provided response...', 'I infer...', 'The JSON contains...', 'The recommendationAgent...'. Instead, use phrases like 'Our analysis indicates...', 'The current operational data suggests...', 'Based on the available operational intelligence...'.\n"
            "6. OPERATIONAL RESPONSE STRUCTURE: For operational questions, always structure the answer naturally. When applicable, include the following sections:\n"
            "   - **Summary**\n"
            "   - **Root Cause**\n"
            "   - **Affected Services**\n"
            "   - **Business Impact**\n"
            "   - **Recommended Actions**\n"
            "   - **Confidence**\n"
            "   Only include sections that are relevant, and ensure they are written in conversational enterprise language.\n"
            "7. You MUST format your response as a valid JSON object. Do not include any explanation or markdown formatting outside of the JSON block.\n\n"
            "The JSON object must have EXACTLY the following keys:\n"
            "{\n"
            '  "reply": "A detailed, natural conversational markdown response answering the user\'s question and structured with the relevant sections (Summary, Root Cause, etc.) as described above.",\n'
            '  "rootCause": "A concise description of the likely root cause of the incident or anomaly based on the context, or \'Unknown\' if context is insufficient.",\n'
            '  "affectedServices": ["List of service names or pipeline names affected by this issue, or empty list if none."],\n'
            '  "businessImpact": "A description of the business unit, application, or SLA impact of the issue based on the context.",\n'
            '  "suggestedActions": ["List of specific, actionable steps to resolve the issue based on the context recommendations, or general troubleshooting steps if unavailable."],\n'
            '  "followUpQuestions": ["1 or 2 relevant follow-up questions the operator might ask next."]\n'
            "}\n"
        )
        
        # Conversation history mapping
        history_str = ""
        if history_list:
            for msg in history_list[-5:]:
                role = "User" if msg.get("role") == "user" else "Copilot"
                history_str += f"{role}: {msg.get('content') or msg.get('text')}\n"
        else:
            history_str = "No previous conversation.\n"
            
        # Format the context string for LLM
        context_str = json.dumps(context, indent=2)
        
        prompt = (
            f"{system_instructions}\n"
            f"=== CONVERSATION HISTORY ===\n{history_str}\n"
            f"=== OPERATIONAL INTELLIGENCE CONTEXT ===\n{context_str}\n"
            f"Current User Question: {question}\n\n"
            f"Response JSON:"
        )
        
        logger.info(f"Final Prompt:\n{prompt}")
        
        # 7. Call local Ollama model
        try:
            config = LLMConfig()
            ollama_config = config.get_ollama_config()
            ollama_url = f"{ollama_config.get('base_url', 'http://localhost:11434').rstrip('/')}/api/generate"
            model_name = ollama_config.get("model", "llama3.2:3b")
            temperature = ollama_config.get("temperature", 0.2)
            timeout = max(ollama_config.get("timeout_seconds", 30), 90)
            
            logger.info(f"Ollama Model: {model_name}")
            
            payload_data = {
                "model": model_name,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature
                }
            }
            
            response_raw = requests.post(ollama_url, json=payload_data, timeout=timeout)
            response_raw.raise_for_status()
            
            response_body = response_raw.json()
            raw_text = response_body.get("response", "").strip()
            logger.info(f"Raw LLM Response: {raw_text}")
            
            # Parse response text
            parsed_json = {}
            first_brace = raw_text.find('{')
            last_brace = raw_text.rfind('}')
            if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
                json_str = raw_text[first_brace:last_brace+1]
                try:
                    parsed_json = json.loads(json_str)
                except Exception as je:
                    logger.warning(f"Failed standard JSON parse on Ollama output: {je}. Attempting literal parse.")
                    try:
                        py_text = json_str.replace("true", "True").replace("false", "False").replace("null", "None")
                        import ast
                        evaluated = ast.literal_eval(py_text)
                        if isinstance(evaluated, dict):
                            parsed_json = evaluated
                    except Exception:
                        pass
            
            # Extract fields
            if parsed_json:
                ans = parsed_json.get("reply") or parsed_json.get("answer") or parsed_json.get("summary") or raw_text
                root_cause = parsed_json.get("rootCause") or "Unknown"
                affected_services = parsed_json.get("affectedServices") or []
                business_impact = parsed_json.get("businessImpact") or "Unknown"
                suggested_actions = parsed_json.get("suggestedActions") or parsed_json.get("recommendedActions") or []
                follow_up = parsed_json.get("followUpQuestions") or []
            else:
                ans = raw_text
                root_cause = "Unknown"
                affected_services = []
                business_impact = "Unknown"
                suggested_actions = []
                follow_up = []
            
            # Update local session history list
            new_history = list(history_list)
            new_history.append({"role": "user", "content": question})
            new_history.append({"role": "assistant", "content": ans})
            session["history"] = new_history[-10:] # keep last 10 turns
            
            # Format API response JSON
            response_time = time.time() - start_time
            response_dict = {
                "reply": ans,
                "rootCause": root_cause,
                "affectedServices": affected_services,
                "businessImpact": business_impact,
                "suggestedActions": suggested_actions,
                "confidence": confidence_val,
                "followUpQuestions": follow_up,
                "agentSources": agent_sources
            }
            
            logger.info(f"Parsed JSON: {json.dumps(parsed_json, indent=2)}")
            logger.info(f"API Response: {json.dumps(response_dict, indent=2)}")
            logger.info(f"LLM Response Time: {response_time:.4f}s")
            
            return response_dict
            
        except requests.exceptions.RequestException as re_err:
            logger.error(f"Ollama local server unreachable: {re_err}")
            logger.error(traceback.format_exc())
            
            response_time = time.time() - start_time
            error_response = {
                "reply": "Error: The local inference service (Ollama Llama 3.2) is unavailable. Please verify that Ollama is running locally on http://localhost:11434 and the 'llama3.2:3b' model is loaded.",
                "rootCause": f"Ollama unreachable: {re_err}",
                "affectedServices": [],
                "businessImpact": "AI Copilot capabilities are currently offline.",
                "suggestedActions": [
                    "Start Ollama locally",
                    "Run 'ollama run llama3.2:3b' in a new terminal window",
                    "Verify http://localhost:11434 is accessible"
                ],
                "confidence": 0.0,
                "followUpQuestions": [],
                "agentSources": []
            }
            logger.info(f"API Response (Error): {json.dumps(error_response, indent=2)}")
            logger.info(f"LLM Response Time (Error): {response_time:.4f}s")
            return error_response
            
        except Exception as e:
            logger.error(f"Unexpected error in CopilotService: {e}")
            logger.error(traceback.format_exc())
            
            response_time = time.time() - start_time
            error_response = {
                "reply": f"Error: An unexpected error occurred while executing the query. Backend processing details could not be successfully resolved.",
                "rootCause": f"Internal server error: {e}",
                "affectedServices": [],
                "businessImpact": "Copilot request processing failed.",
                "suggestedActions": [
                    "Check the backend console for the full exception stack trace",
                    "Verify the operational database files are present",
                    "Restart the FastAPI application"
                ],
                "confidence": 0.0,
                "followUpQuestions": [],
                "agentSources": []
            }
            logger.info(f"API Response (Error): {json.dumps(error_response, indent=2)}")
            logger.info(f"LLM Response Time (Error): {response_time:.4f}s")
            return error_response
