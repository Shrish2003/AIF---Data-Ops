import os
import json
import logging
import uuid
from typing import Dict, Any, List, Optional
import requests
from pydantic import BaseModel, Field, ValidationError

import threading
from collections import OrderedDict

logger = logging.getLogger(__name__)

# Bounded Query Embedding Cache (Phase 3.1)
_cache_lock = threading.Lock()
_embedding_cache = OrderedDict()

# Reusable global HTTP session for connection pooling
_http_session = requests.Session()

def is_cache_enabled() -> bool:
    return os.getenv("OPERATIONAL_MEMORY_EMBED_CACHE_ENABLED", "true").lower() in ("true", "1", "yes")

def get_cache_size_limit() -> int:
    try:
        return int(os.getenv("OPERATIONAL_MEMORY_EMBED_CACHE_SIZE", "128"))
    except Exception:
        return 128

# ==========================================================
# Operational Memory Schema Validation Model
# ==========================================================
class OperationalMemoryRecord(BaseModel):
    memory_id: Optional[str] = Field(None, description="Unique identifier for the stored memory")
    incident_id: str = Field(..., description="ID of the associated incident")
    pipeline_id: str = Field(..., description="ID of the data pipeline affected")
    timestamp: str = Field(..., description="ISO or standard timestamp of the event")
    incident_type: str = Field(..., description="Category or classification of the incident")
    summary: str = Field(..., description="High-level narrative of what happened")
    symptoms: str = Field(..., description="Observable symptoms in metrics or logs")
    behavior: str = Field(..., description="Behavioral anomalies recorded")
    root_cause: str = Field(..., description="Identified root cause")
    risk: str = Field(..., description="Cascading risk level or severity")
    affected_services: List[str] = Field(..., description="List of services downstream or directly affected")
    business_impact: str = Field(..., description="SLA or business value impact statement")
    recommendation: str = Field(..., description="System proposed playbook remediation")
    action_taken: str = Field(..., description="Remediations executed by the operator")
    outcome: str = Field(..., description="Outcome of the recovery action")
    resolution_status: str = Field(..., description="Final resolution status (e.g. RESOLVED, CLOSED)")


# ==========================================================
# Semantic Document Generator Helper
# ==========================================================
def create_semantic_document(incident: Dict[str, Any]) -> str:
    """
    Constructs a rich, natural language semantic document out of structured incident details.
    This paragraph represents the textual query target embedded into ChromaDB.
    """
def create_semantic_document(incident: Dict[str, Any]) -> str:
    """
    Compiles structured incident fields into a natural language paragraph
    suitable for vector embeddings, using the specified format and excluding
    empty/unavailable fields.
    """
    def is_valid(val):
        if val is None:
            return False
        if isinstance(val, list):
            return len(val) > 0 and any(is_valid(item) for item in val)
        val_str = str(val).strip().lower()
        return val_str not in ("", "none", "null", "not available", "not determined", "unknown", "n/a")

    parts = []
    
    # 1. Pipeline
    pipeline_id = incident.get("pipeline_id")
    if is_valid(pipeline_id):
        parts.append(f"Pipeline:\n{pipeline_id}")
        
    # 2. Incident
    summary = incident.get("summary")
    if is_valid(summary):
        parts.append(f"Incident:\n{summary}")
        
    # 3. Observed Symptoms
    symptoms = incident.get("symptoms")
    if is_valid(symptoms):
        parts.append(f"Observed Symptoms:\n{symptoms}")
        
    # 4. Behavior
    behavior = incident.get("behavior")
    if is_valid(behavior):
        parts.append(f"Behavior:\n{behavior}")
        
    # 5. Risk
    risk = incident.get("risk")
    if is_valid(risk):
        parts.append(f"Risk:\n{risk}")
        
    # 6. Affected Services
    affected_services = incident.get("affected_services")
    if isinstance(affected_services, list):
        valid_services = [s for s in affected_services if is_valid(s)]
        affected_services_str = ", ".join(valid_services)
    else:
        affected_services_str = str(affected_services) if affected_services else ""
        
    if is_valid(affected_services_str):
        parts.append(f"Affected Services:\n{affected_services_str}")
        
    # 7. Business Context
    business_impact = incident.get("business_impact")
    if is_valid(business_impact):
        parts.append(f"Business Context:\n{business_impact}")
        
    # 8. Recommendation
    recommendation = incident.get("recommendation")
    if is_valid(recommendation):
        parts.append(f"Recommendation:\n{recommendation}")
        
    # 9. Outcome
    outcome = incident.get("outcome")
    if is_valid(outcome):
        parts.append(f"Outcome:\n{outcome}")
        
    return "\n\n".join(parts)


# ==========================================================
# Operational Memory Service Implementation
# ==========================================================
class MemoryService:
    """
    ==========================================================
    Operational Memory Service

    Responsibility:
        Provide a clean, isolated persistence and semantic search
        capability over operational incidents using a local ChromaDB
        instance. It leverages Ollama for local text embeddings.

    Design constraints:
        - Must be optional. If ChromaDB or Ollama are unavailable,
          no crashes will propagate to the rest of the application.
        - List fields are serialized to JSON strings in metadata
          due to ChromaDB requirements.
    ==========================================================
    """
    
    def __init__(self, collection_name: str = "operational_memory"):
        self._client = None
        self._collection = None
        self.is_available = False
        self._init_error = None
        
        self.persist_dir = os.getenv("CHROMADB_PERSIST_DIR", "data/chromadb")
        self.collection_name = collection_name
        
        # Self-initialize on construction
        self.initialize_memory()

    # =====================================================
    # INITIALIZE CHROMADB
    # =====================================================
    def initialize_memory(self) -> bool:
        """
        Initializes the local ChromaDB database. Ensures the operational_memory
        collection exists and is active.
        """
        try:
            import chromadb
            
            # Ensure parent directories exist
            os.makedirs(self.persist_dir, exist_ok=True)
            
            # Initialize persistent client
            self._client = chromadb.PersistentClient(path=self.persist_dir)
            
            # Retrieve or create collection using cosine distance metric
            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"}
            )
            
            self.is_available = True
            logger.info(f"Operational Memory initialized. Persist Directory: {self.persist_dir}")
            
            # Optional model warm-up (Phase 3.1)
            warmup_enabled = os.getenv("OLLAMA_EMBEDDING_WARMUP_ENABLED", "false").lower() in ("true", "1", "yes")
            if warmup_enabled:
                self._warmup_model()
                
            return True
            
        except ImportError as e:
            self.is_available = False
            self._init_error = str(e)
            logger.warning("chromadb library is not installed. Operational Memory is offline.")
            return False
        except Exception as e:
            self.is_available = False
            self._init_error = str(e)
            self._client = None
            self._collection = None
            logger.warning(f"Failed to initialize ChromaDB. Operational Memory is offline: {e}")
            return False

    def _warmup_model(self) -> None:
        """
        Warms up the Ollama embedding model by sending a dummy request.
        """
        try:
            logger.info("Warming up Ollama embedding model...")
            # Use a dummy text to trigger model load
            self._generate_embedding("warmup query")
            logger.info("Ollama embedding model warm-up completed successfully.")
        except Exception as e:
            logger.warning(f"Ollama embedding model warm-up failed (non-fatal): {e}")

    # =====================================================
    # EMBEDDING GENERATION
    # =====================================================
    def _generate_embedding(self, text: str) -> List[float]:
        """
        Queries the local Ollama instance for text embeddings.
        Returns a vector list of floats.
        Uses thread-safe in-process LRU cache and HTTP session pooling.
        """
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        model = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")
        
        cache_key = text.strip()
        cache_enabled = is_cache_enabled()
        
        # 1. Bounded LRU Cache Check
        if cache_enabled:
            with _cache_lock:
                if cache_key in _embedding_cache:
                    logger.debug(f"Embedding Cache HIT for: '{cache_key[:60]}...'")
                    _embedding_cache.move_to_end(cache_key)
                    return _embedding_cache[cache_key]
                    
        # 2. HTTP Session Connection Pool
        url = f"{base_url.rstrip('/')}/api/embeddings"
        payload = {
            "model": model,
            "prompt": text
        }
        
        try:
            logger.debug(f"Generating embedding for text: '{text[:60]}...' using model {model}")
            response = _http_session.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                embedding = response.json().get("embedding")
                if embedding:
                    # Save to cache
                    if cache_enabled:
                        with _cache_lock:
                            _embedding_cache[cache_key] = embedding
                            limit = get_cache_size_limit()
                            if len(_embedding_cache) > limit:
                                _embedding_cache.popitem(last=False)
                    return embedding
                    
            # Fallback to alternative /api/embed endpoint
            url_alt = f"{base_url.rstrip('/')}/api/embed"
            payload_alt = {
                "model": model,
                "input": text
            }
            response_alt = _http_session.post(url_alt, json=payload_alt, timeout=30)
            if response_alt.status_code == 200:
                embeddings = response_alt.json().get("embeddings")
                if embeddings and len(embeddings) > 0:
                    embedding_alt = embeddings[0]
                    # Save to cache
                    if cache_enabled:
                        with _cache_lock:
                            _embedding_cache[cache_key] = embedding_alt
                            limit = get_cache_size_limit()
                            if len(_embedding_cache) > limit:
                                _embedding_cache.popitem(last=False)
                    return embedding_alt
                    
            raise ValueError(f"Ollama response missing embedding. Status: {response.status_code}, Body: {response.text}")
            
        except Exception as e:
            logger.error(f"Failed to generate embedding via Ollama: {e}")
            raise

    # =====================================================
    # ADD MEMORY
    # =====================================================
    def add_memory(self, incident: Dict[str, Any]) -> Optional[str]:
        """
        Validates an incident dictionary, generates its semantic document,
        queries Ollama for the text embedding, and stores the record in ChromaDB.
        """
        if not self.is_available or self._collection is None:
            logger.warning("Operational Memory is offline. Skipping add_memory.")
            return None
            
        try:
            # 1. Type validation via Pydantic
            record = OperationalMemoryRecord(**incident)
            
            # Generate a memory ID if not provided
            if not record.memory_id:
                record.memory_id = f"mem_{uuid.uuid4().hex[:8]}"
                
            # 2. Build semantic document
            doc_text = create_semantic_document(record.model_dump())
            
            # 3. Generate embedding vector
            embedding = self._generate_embedding(doc_text)
            
            # 4 & 5. Compile metadata (serializing list fields for ChromaDB compatibility)
            metadata = {
                "incident_id": record.incident_id,
                "pipeline_id": record.pipeline_id,
                "timestamp": record.timestamp,
                "incident_type": record.incident_type,
                "risk": record.risk,
                "affected_services": json.dumps(record.affected_services),
                "business_impact": record.business_impact,
                "resolution_status": record.resolution_status,
                "summary": record.summary,
                "symptoms": record.symptoms,
                "behavior": record.behavior,
                "root_cause": record.root_cause,
                "recommendation": record.recommendation,
                "action_taken": record.action_taken,
                "outcome": record.outcome
            }
            
            # Incorporate extra metadata fields for source traceability (excluding list/dict types)
            core_fields = set(OperationalMemoryRecord.model_fields.keys())
            for key, value in incident.items():
                if key not in core_fields and isinstance(value, (str, int, float, bool)):
                    metadata[key] = value
            
            # Store in ChromaDB
            self._collection.add(
                ids=[record.memory_id],
                documents=[doc_text],
                embeddings=[embedding],
                metadatas=[metadata]
            )
            
            logger.info(f"Stored operational memory ID: {record.memory_id} (Incident: {record.incident_id})")
            return record.memory_id
            
        except ValidationError as ve:
            logger.error(f"Incident validation failed: {ve}")
            raise ValueError(f"Invalid incident schema representation: {ve}")
        except Exception as e:
            logger.error(f"Failed to store memory: {e}")
            raise

    # =====================================================
    # SEARCH SIMILAR MEMORIES
    # =====================================================
    def search_similar_memories(self, query: str, limit: int = 5, where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Embeds the natural-language query and searches the ChromaDB store.
        Returns a list of structured results along with metadata and distances.
        Supports structured metadata filtering via the where argument.
        """
        if not self.is_available or self._collection is None:
            logger.warning("Operational Memory is offline. Skipping search.")
            return []
            
        if not query or not query.strip():
            logger.warning("Empty search query provided.")
            return []
            
        try:
            # 1. Embed query
            query_embedding = self._generate_embedding(query)
            
            # 2. Query vector store
            query_args = {
                "query_embeddings": [query_embedding],
                "n_results": limit
            }
            if where:
                query_args["where"] = where
                
            results = self._collection.query(**query_args)
            
            # 3. Format outputs
            formatted = []
            if results and "ids" in results and results["ids"]:
                ids = results["ids"][0]
                docs = results["documents"][0]
                metadatas = results["metadatas"][0]
                distances = results.get("distances", [[]])[0]
                
                for idx in range(len(ids)):
                    metadata = metadatas[idx]
                    
                    # Deserialize lists
                    affected_services_str = metadata.get("affected_services", "[]")
                    try:
                        affected_services = json.loads(affected_services_str)
                    except Exception:
                        affected_services = [affected_services_str]
                        
                    formatted.append({
                        "memory_id": ids[idx],
                        "document": docs[idx],
                        "distance": distances[idx] if idx < len(distances) else 1.0,
                        "metadata": {
                            "incident_id": metadata.get("incident_id"),
                            "pipeline_id": metadata.get("pipeline_id"),
                            "timestamp": metadata.get("timestamp"),
                            "incident_type": metadata.get("incident_type"),
                            "risk": metadata.get("risk"),
                            "affected_services": affected_services,
                            "business_impact": metadata.get("business_impact"),
                            "resolution_status": metadata.get("resolution_status"),
                            "summary": metadata.get("summary"),
                            "symptoms": metadata.get("symptoms"),
                            "behavior": metadata.get("behavior"),
                            "root_cause": metadata.get("root_cause"),
                            "recommendation": metadata.get("recommendation"),
                            "action_taken": metadata.get("action_taken"),
                            "outcome": metadata.get("outcome")
                        }
                    })
                    
                    # Fetch extra metadata fields stored for traceability
                    memory_metadata = formatted[-1]["metadata"]
                    for key, value in metadata.items():
                        if key not in memory_metadata and key != "affected_services":
                            memory_metadata[key] = value
                    
            logger.info(f"Retrieved {len(formatted)} memories similar to query '{query[:40]}'")
            return formatted
            
        except Exception as e:
            logger.error(f"Error during semantic memory search: {e}")
            return []

    # =====================================================
    # GET MEMORY
    # =====================================================
    def get_memory(self, memory_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single memory record by its ID.
        """
        if not self.is_available or self._collection is None:
            logger.warning("Operational Memory is offline. Skipping get_memory.")
            return None
            
        try:
            result = self._collection.get(ids=[memory_id])
            if result and result.get("ids"):
                idx = 0
                metadata = result["metadatas"][idx]
                
                affected_services_str = metadata.get("affected_services", "[]")
                try:
                    affected_services = json.loads(affected_services_str)
                except Exception:
                    affected_services = [affected_services_str]
                    
                memory_metadata = {
                    "incident_id": metadata.get("incident_id"),
                    "pipeline_id": metadata.get("pipeline_id"),
                    "timestamp": metadata.get("timestamp"),
                    "incident_type": metadata.get("incident_type"),
                    "risk": metadata.get("risk"),
                    "affected_services": affected_services,
                    "business_impact": metadata.get("business_impact"),
                    "resolution_status": metadata.get("resolution_status"),
                    "summary": metadata.get("summary"),
                    "symptoms": metadata.get("symptoms"),
                    "behavior": metadata.get("behavior"),
                    "root_cause": metadata.get("root_cause"),
                    "recommendation": metadata.get("recommendation"),
                    "action_taken": metadata.get("action_taken"),
                    "outcome": metadata.get("outcome")
                }
                
                # Fetch extra metadata fields stored for traceability
                for key, value in metadata.items():
                    if key not in memory_metadata and key != "affected_services":
                        memory_metadata[key] = value
                        
                return {
                    "memory_id": result["ids"][idx],
                    "document": result["documents"][idx] if result.get("documents") else "",
                    "metadata": memory_metadata
                }
            return None
        except Exception as e:
            logger.error(f"Error fetching memory ID {memory_id}: {e}")
            return None

    # =====================================================
    # DELETE MEMORY
    # =====================================================
    def delete_memory(self, memory_id: str) -> bool:
        """
        Deletes a single memory record from ChromaDB by its ID.
        """
        if not self.is_available or self._collection is None:
            logger.warning("Operational Memory is offline. Skipping delete_memory.")
            return False
            
        try:
            self._collection.delete(ids=[memory_id])
            logger.info(f"Deleted operational memory ID: {memory_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete memory ID {memory_id}: {e}")
            return False

    # =====================================================
    # HEALTH CHECK
    # =====================================================
    def health_check(self) -> Dict[str, Any]:
        """
        Performs a health check of both ChromaDB and the local Ollama embedding service.
        """
        chromadb_status = "unavailable"
        ollama_status = "unavailable"
        embedding_model = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")
        
        # Check ChromaDB
        if self.is_available and self._collection is not None:
            try:
                self._collection.count()
                chromadb_status = "available"
            except Exception as e:
                chromadb_status = f"error: {e}"
        else:
            if self._init_error:
                chromadb_status = f"error: {self._init_error}"
            else:
                chromadb_status = "unavailable"
                
        # Check Ollama
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        try:
            response = requests.get(f"{base_url.rstrip('/')}/api/tags", timeout=5)
            if response.status_code == 200:
                models = [m.get("name") for m in response.json().get("models", [])]
                if any(embedding_model in m or m in embedding_model for m in models):
                    ollama_status = "available"
                else:
                    ollama_status = f"model_missing: required embedding model '{embedding_model}' not loaded. Available models: {models}"
            else:
                ollama_status = f"unexpected_http_status: {response.status_code}"
        except Exception as e:
            ollama_status = f"offline: {e}"
            
        overall_status = "healthy" if (chromadb_status == "available" and ollama_status == "available") else "degraded"
        if chromadb_status != "available" and ollama_status != "available":
            overall_status = "unhealthy"
            
        return {
            "status": overall_status,
            "chromadb": {
                "status": chromadb_status,
                "persist_directory": self.persist_dir,
                "collection": self.collection_name
            },
            "ollama_embeddings": {
                "status": ollama_status,
                "endpoint": f"{base_url.rstrip('/')}/api/embeddings",
                "configured_model": embedding_model
            }
        }
