import os
import json
import logging
from dotenv import load_dotenv

# Load env variables from .env
load_dotenv()

# Set up logging to print to console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("verify_operational_memory")

from services.memory_service import MemoryService
from services.memory_ingestion import ingest_real_operational_data

def verify_memory_flow():
    logger.info("=" * 60)
    logger.info("OPERATIONAL SEMANTIC MEMORY REAL DATA VERIFICATION")
    logger.info("=" * 60)
    
    # 1. First Ingestion Run
    logger.info("Running Ingestion on REAL operational data (Capped at 50 records for speed)...")
    stats1 = ingest_real_operational_data(max_records=50, collection_name="operational_memory")
    logger.info(f"Ingestion Run 1 Statistics:\n{json.dumps(stats1, indent=2)}")
    
    # 2. Second Ingestion Run (Verify Idempotency/Duplicate skipping)
    logger.info("\nRunning Ingestion AGAIN to verify duplicate prevention...")
    stats2 = ingest_real_operational_data(max_records=50, collection_name="operational_memory")
    logger.info(f"Ingestion Run 2 Statistics:\n{json.dumps(stats2, indent=2)}")
    
    # Initialize Memory Service to perform retrieval
    service = MemoryService(collection_name="operational_memory")
    if not service.is_available:
        logger.error("ChromaDB is unavailable. Cannot perform search verification.")
        return
        
    # 3. Perform semantic queries targeting real incidents
    queries = [
        "Kafka consumer lag or offset processing delay",
        "SLA task timeout or execution runtime exceed lock contention",
        "Pod crash restart or Kubernetes namespace error",
        "Integrity record count check schema validation mismatch"
    ]
    
    logger.info("\nPerforming Semantic Queries on Real Operational Memory...")
    for q in queries:
        logger.info(f"\n--- Query: '{q}' ---")
        try:
            results = service.search_similar_memories(q, limit=2)
            if not results:
                logger.info("No matching memories found.")
            for r in results:
                metadata = r['metadata']
                logger.info(f"Memory ID       : {r['memory_id']}")
                logger.info(f"Incident ID     : {metadata.get('incident_id')}")
                logger.info(f"Pipeline ID     : {metadata.get('pipeline_id')}")
                logger.info(f"Type            : {metadata.get('incident_type')}")
                logger.info(f"Severity/Risk   : {metadata.get('risk')}")
                logger.info(f"Root Cause      : {metadata.get('root_cause')}")
                logger.info(f"Resolution      : {metadata.get('action_taken')}")
                logger.info(f"Distance        : {r['distance']:.4f}")
                
                # Traceability print
                logger.info("Traceability Metadata:")
                logger.info(f"  • Source Dataset   : {metadata.get('source_dataset')}")
                logger.info(f"  • Source Record ID : {metadata.get('source_record_id')}")
                logger.info(f"  • Source Timestamp : {metadata.get('source_timestamp')}")
                logger.info(f"  • Created At       : {metadata.get('created_at')}")
                logger.info(f"  • Memory Version   : {metadata.get('memory_version')}")
                
                logger.info(f"Document        :\n{r['document']}")
                logger.info("-" * 40)
        except Exception as e:
            logger.error(f"Search failed for query '{q}': {e}")
            
    logger.info("=" * 60)
    logger.info("REAL DATA VERIFICATION COMPLETED")
    logger.info("=" * 60)

if __name__ == "__main__":
    verify_memory_flow()
