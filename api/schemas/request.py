from typing import Any, Dict, Optional
from pydantic import BaseModel

class CopilotQuery(BaseModel):
    useCaseId: str
    domainId: str
    question: str
    context: Optional[Dict[str, Any]] = None

