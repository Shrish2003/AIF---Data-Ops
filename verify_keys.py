import json

with open("output/execution_output.json", "r", encoding="utf-8") as f:
    data = json.load(f)

identifier_keys = {"entity_id", "pipeline_id", "id", "name", "identifier"}

for category, datasets in data.items():
    if not isinstance(datasets, dict):
        continue
    print(f"\nCategory: {category}")
    for dataset_name, items in datasets.items():
        if not items:
            continue
        if isinstance(items, dict):
            continue
        first_item = items[0]
        found_keys = [k for k in first_item.keys() if k in identifier_keys]
        print(f"  - Dataset '{dataset_name}': keys found: {found_keys} (all keys: {list(first_item.keys())[:5]})")
