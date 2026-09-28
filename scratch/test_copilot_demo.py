import os
import sys
sys.path.insert(0, os.path.abspath("."))
import json
import time

from dotenv import load_dotenv
load_dotenv()

from api.schemas.request import CopilotQuery
from api.services.copilot_service import CopilotService
from api.services.context_builder import OperationalIntelligenceContextBuilder

def main():
    print("==========================================================")
    print("COPILOT END-TO-END VALIDATION (10 QUESTIONS + 1 HALLUCINATION CHECK)")
    print("==========================================================")
    
    ctx = OperationalIntelligenceContextBuilder.build_context("BRO0001", "data-ops", "default")
    
    questions = [
        "What happened with BRO0001?",
        "What is the current health of this pipeline?",
        "Have we seen similar incidents before?",
        "What are the likely root causes?",
        "What services could be affected?",
        "What is the business impact?",
        "What recommendation is being suggested?",
        "Why is this recommendation being suggested?",
        "What historical evidence supports it?",
        "What evidence supports your conclusion?",
        # 11. Hallucination check question
        "What exact engineer caused this failure and what was their employee badge ID?"
    ]
    
    for i, q in enumerate(questions, 1):
        payload = CopilotQuery(
            useCaseId="data-ops",
            domainId="default",
            question=q,
            context=ctx
        )
        t0 = time.perf_counter()
        resp = CopilotService.chat(payload)
        dur = (time.perf_counter() - t0) * 1000
        
        reply = resp.get("reply", "")
        root_cause = resp.get("rootCause", "")
        biz_impact = resp.get("businessImpact", "")
        affected = resp.get("affectedServices", [])
        actions = resp.get("suggestedActions", [])
        
        label = "HALLUCINATION CHECK" if i == 11 else f"Q{i}"
        print(f"\n[{label}] \"{q}\"")
        print(f"Confidence: {resp.get('confidence')}, Duration: {dur:.1f}ms")
        print(f"Root Cause Field: {root_cause}")
        print(f"Business Impact Field: {biz_impact}")
        print(f"Affected Services: {affected}")
        print(f"Suggested Actions: {actions}")
        print(f"Reply Excerpt:\n{reply[:300]}...")

if __name__ == "__main__":
    main()
