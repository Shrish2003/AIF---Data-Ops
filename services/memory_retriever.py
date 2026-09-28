import os
from typing import Dict, Any, List
import logging
from services.memory_service import MemoryService

logger = logging.getLogger(__name__)

def is_memory_enabled() -> bool:
    val = os.getenv("OPERATIONAL_MEMORY_ENABLED", "false").lower()
    return val in ("true", "1", "yes")

def get_top_k() -> int:
    try:
        return int(os.getenv("OPERATIONAL_MEMORY_TOP_K", "3"))
    except Exception:
        return 3

def get_distance_threshold() -> float:
    try:
        return float(os.getenv("OPERATIONAL_MEMORY_DISTANCE_THRESHOLD", "0.50"))
    except Exception:
        return 0.50

class OperationalMemoryRetriever:
    """
    ==========================================================
    Operational Memory Retriever Adapter
    
    Responsibility:
        - Build a semantic query based on current operational telemetry.
        - Retrieve similar incidents from persistent ChromaDB collection.
        - Apply similarity distance thresholds and Top-K caps.
        - Normalize retrieved historical matches into a standard structure.
        - Fallback gracefully when memory is disabled or services are offline.
    ==========================================================
    """

    @staticmethod
    def build_retrieval_query(behavior_object: Dict[str, Any]) -> str:
        """
        Constructs a detailed semantic query from verified telemetry fields.
        """
        entity_id = behavior_object.get("entity_id", "Unknown")
        entity_name = behavior_object.get("entity_name", "Unknown")
        entity_type = behavior_object.get("entity_type", "Unknown")
        source_system = behavior_object.get("source_system", "Unknown")
        behavior = behavior_object.get("behavior", {})
        severity = behavior.get("severity", "Unknown")
        patterns = behavior.get("patterns", [])
        
        # Pull Observer Events
        obs_obj = behavior_object.get("observation", {})
        events = obs_obj.get("observations", {}).get("events", [])
        events_str = ", ".join(events) if events else ""
        
        # Build symptoms from deviations
        deviations = behavior.get("deviation", {})
        symptom_parts = []
        for metric, dev_info in deviations.items():
            sev = dev_info.get("severity")
            if sev in ("WARNING", "CRITICAL"):
                pct = dev_info.get("deviation_percent", 0.0)
                symptom_parts.append(f"{metric} deviated by {pct}%")
                
        s_str = ", ".join(symptom_parts)
        
        parts = []
        parts.append(f"Incident on {source_system} {entity_type} '{entity_name}' (ID: {entity_id}).")
        parts.append(f"Behavior status is {severity}.")
        if patterns:
            parts.append(f"Anomaly patterns: {', '.join(patterns)}.")
        if s_str:
            parts.append(f"Observed deviations: {s_str}.")
        if events_str:
            parts.append(f"Trigger events: {events_str}.")
            
        return " ".join(parts).strip()

    @classmethod
    def retrieve_historical_evidence(cls, behavior_object: Dict[str, Any], collection_name: str = "operational_memory") -> Dict[str, Any]:
        """
        Queries ChromaDB vector database and returns normalized matches.
        Ensures strict separation and failure isolation.
        """
        disabled_response = {
            "available": False,
            "retrieval_query": "",
            "retrieval_count": 0,
            "matches": []
        }
        
        if not is_memory_enabled():
            logger.debug("Operational Memory integration is disabled via OPERATIONAL_MEMORY_ENABLED flag.")
            return disabled_response
            
        try:
            query = cls.build_retrieval_query(behavior_object)
            if not query:
                return disabled_response
                
            top_k = get_top_k()
            threshold = get_distance_threshold()
            
            # Initialize service
            service = MemoryService(collection_name=collection_name)
            if not service.is_available:
                logger.warning("ChromaDB or Ollama embedding service is not available.")
                return disabled_response
                
            # Perform query
            results = service.search_similar_memories(query, limit=top_k)
            if not results:
                return disabled_response
                
            matches = []
            for r in results:
                distance = r.get("distance", 1.0)
                # Cosine distance: lower values represent closer match. Filter out anything above threshold.
                if distance > threshold:
                    logger.debug(f"Discarding match {r['memory_id']} due to threshold check (Distance: {distance:.4f} > Threshold: {threshold:.4f})")
                    continue
                    
                metadata = r.get("metadata", {})
                normalized_match = {
                    "memory_id": r["memory_id"],
                    "incident_id": metadata.get("incident_id") or "Not determined",
                    "source_dataset": metadata.get("source_dataset") or "Unknown",
                    "source_record_id": metadata.get("source_record_id"),
                    "source_timestamp": metadata.get("source_timestamp"),
                    "source_system": metadata.get("source_system") or "Unknown",
                    "pipeline_id": metadata.get("pipeline_id") or "Not determined",
                    "incident_type": metadata.get("incident_type") or "Unknown",
                    "distance": distance,
                    "root_cause": metadata.get("root_cause"),
                    "recommendation": metadata.get("recommendation"),
                    "action_taken": metadata.get("action_taken"),
                    "outcome": metadata.get("outcome"),
                    "resolution_status": metadata.get("resolution_status")
                }
                matches.append(normalized_match)
                
            if not matches:
                return disabled_response
                
            return {
                "available": True,
                "retrieval_query": query,
                "retrieval_count": len(matches),
                "matches": matches
            }
            
        except Exception as e:
            logger.warning(f"Operational Memory retrieval failed gracefully: {e}")
            return disabled_response
