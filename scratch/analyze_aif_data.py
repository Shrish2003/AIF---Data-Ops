import os
import sys

# Ensure project root is in PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/.."))

from adapters.connectors.csv_connector import CSVConnector
from adapters.parsers.tabular_parser import TabularParser

def main():
    connector = CSVConnector("config/sources.yaml")
    connector.connect()
    datasets = connector.read_all()
    connector.disconnect()
    
    lookup_dataset_names = ["business_context", "historical_baselines", "incident_history", "pipeline_lineage", "recommendation_history"]
    lookup_datasets = {name: datasets[name] for name in lookup_dataset_names}
    
    parser = TabularParser()
    parsed_lookup = parser.parse_all(lookup_datasets)
    
    df = parsed_lookup["business_context"]
    
    systems = set(r.get("source_system") for r in df if r.get("source_system"))
    print("--- Lookup ID patterns by source system ---")
    for sys_name in systems:
        sys_records = [r for r in df if r.get("source_system") == sys_name]
        ids = list(set(r.get("entity_id") for r in sys_records if r.get("entity_id")))
        print(f"System: {sys_name:25} | Records: {len(sys_records):5} | Sample IDs: {ids[:5]}")
        
if __name__ == "__main__":
    main()
