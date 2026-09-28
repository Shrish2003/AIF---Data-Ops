import json
from services.memory_service import MemoryService

def main():
    service = MemoryService(collection_name="operational_memory")
    if not service.is_available:
        print("Error: Memory Service is offline.")
        return
        
    queries = [
        # 1. Kafka consumer lag
        "Kafka consumer lag topic partition delay recommendation engine",
        # 2. Airflow pipeline failure
        "Airflow pipeline execution failed task error loyalty sync",
        # 3. SLA/runtime degradation
        "SLA task runtime duration staging load table locked contention",
        # 4. Data quality/schema issue
        "Integrity record count check schema validation mismatch customer zip code",
        # 5. Kubernetes resource problem
        "Kubernetes pod container crash restart analytics aggregator memory leak",
        # 6. SAP dependency/job failure
        "SAP ERP batch job execution failure secondary standby host database sync",
        # 7. Manufacturing anomaly
        "Manufacturing machine welding defect count anomaly shift operator",
        # 8. IoT sensor anomaly
        "IoT device temperature battery low signal strength sensor failure",
        # 9. Pipeline lineage depends downstream
        "Pipeline lineage downstream depends on database staging table",
        # 10. Risk prediction high
        "High risk prediction score SLA breach quality assurance"
    ]
    
    relevance_scores = {
        "top1": 0,
        "top3": 0,
        "top5": 0
    }
    
    print("="*80)
    print("RUNNING 10 VALIDATION RETRIEVAL QUERIES")
    print("="*80)
    
    for idx, q in enumerate(queries):
        print(f"\nQUERY #{idx+1}: '{q}'")
        print("-" * 50)
        
        results = service.search_similar_memories(q, limit=5)
        if not results:
            print("No relevant indexed incident found.")
            continue
            
        top1_relevant = False
        top3_relevant = False
        top5_relevant = False
        
        for rank, r in enumerate(results):
            metadata = r.get("metadata", {})
            distance = r.get("distance", 1.0)
            doc = r.get("document", "")
            
            # Simple keyword relevance validation helper
            is_relevant = False
            
            # Verify dataset context relevance
            ds = str(metadata.get("source_dataset", "")).lower()
            q_lower = q.lower()
            
            if "kafka" in q_lower and ds == "kafka":
                is_relevant = True
            elif "airflow" in q_lower and ds == "airflow":
                is_relevant = True
            elif "sla" in q_lower and ("airflow" in ds or "azure" in ds):
                is_relevant = True
            elif "integrity" in q_lower and ("integrity" in doc.lower() or "validation" in doc.lower() or ds == "manufacturing"):
                is_relevant = True
            elif "kubernetes" in q_lower and ds == "kubernetes":
                is_relevant = True
            elif "sap" in q_lower and ds == "sap erp":
                is_relevant = True
            elif "manufacturing" in q_lower and ds == "manufacturing":
                is_relevant = True
            elif "iot" in q_lower and ds == "iot":
                is_relevant = True
            elif "lineage" in q_lower:
                is_relevant = True
            elif "risk" in q_lower and ("risk" in doc.lower() or "score" in doc.lower()):
                is_relevant = True
                
            # If distance is too high, it might be a weak match, but if keywords align we check
            relevance_str = "YES" if is_relevant else "NO"
            
            if is_relevant:
                if rank == 0:
                    top1_relevant = True
                if rank < 3:
                    top3_relevant = True
                if rank < 5:
                    top5_relevant = True
                    
            print(f"Rank {rank+1} [Relevance: {relevance_str}] (Distance: {distance:.4f})")
            print(f"  Memory ID   : {r['memory_id']}")
            print(f"  Incident ID : {metadata.get('incident_id')}")
            print(f"  Dataset     : {ds}")
            print(f"  Type        : {metadata.get('incident_type')}")
            print(f"  Root Cause  : {metadata.get('root_cause')}")
            print(f"  Doc snippet : {doc.replace('\n', ' ')[:120]}...")
            print()
            
        if top1_relevant:
            relevance_scores["top1"] += 1
        if top3_relevant:
            relevance_scores["top3"] += 1
        if top5_relevant:
            relevance_scores["top5"] += 1
            
    print("="*80)
    print("RELEVANCE EVALUATION SUMMARY")
    print("="*80)
    print(f"Top-1 Relevance: {relevance_scores['top1']}/10")
    print(f"Top-3 Relevance: {relevance_scores['top3']}/10")
    print(f"Top-5 Relevance: {relevance_scores['top5']}/10")
    print("="*80)

if __name__ == "__main__":
    main()
