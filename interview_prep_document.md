# Adaptive Intelligence Fabric (AIF) - Technical Interview Preparation Guide
This preparation document is grounded strictly in the actual codebase implementation of the **Adaptive Intelligence Fabric (AIF) – Data Pipeline Operations Intelligence** repository.

---

## 1. Executive Project Overview
* **Project Name**: Adaptive Intelligence Fabric (AIF) – Data Pipeline Operations Intelligence
* **Business Problem**: Modern enterprises depend on complex, heterogeneous data pipelines (running on Airflow, Kafka, ADF, Kubernetes, and SAP batch jobs) to drive critical business operations. When pipelines fail, run slowly, drift from baseline runtimes, or produce corrupted data, diagnosing the issues is slow, manual, and siloed. This leads to breaches of SLAs (Service Level Agreements), degraded operational integrity, and erosion of data trust.
* **Technical Problem**: Observability metrics, metadata logs, and operational events are highly fragmented and use incompatible schemas depending on the vendor or host platform. Traditional rule-based alerts are too rigid, unable to forecast cascading failure risks, detect subtle behavioral drifts, verify downstream output integrity, or automatically recommend playbooks based on historical context.
* **Operational Gap**: The lack of a unified canonical model that standardizes heterogeneous data pipeline metrics, combined with the lack of a multi-agent reasoning layer capable of contextualizing anomalies, assessing risk, checking data quality, and recommending recovery actions.
* **Proposed Solution (AIF)**: A domain-agnostic operational intelligence platform that ingests raw telemetry via a configuration-driven Capability Adapter, maps and validates it into a Canonical Data Model, runs multi-agent operations diagnostics (Observer, Behavior, Risk, Integrity, and Recommendation Agents), enhances critical findings using local LLMs (Ollama Llama 3.2 3B) backed by structured playbook retrieval, and evaluates recommendation accuracy using RAGAS against Golden Truth records.

---

## 2. 30-Second Project Explanation
> "Adaptive Intelligence Fabric (AIF) is an enterprise operational intelligence platform for data pipelines. It takes raw execution logs and telemetry from Airflow, Kafka, Kubernetes, SAP, and Azure Data Factory, normalizes them into a single canonical data model, and runs a sequence of specialized AI agents to analyze behavior, predict SLA breach risks, verify output integrity, and generate step-by-step troubleshooting playbooks. In short, it automates incident triage and remediation planning across our entire data platform stack to minimize recovery times."

---

## 3. 2-Minute Project Explanation
> "Our project, the Adaptive Intelligence Fabric, bridges the gap between raw data pipeline telemetry and action-oriented operational intelligence. 
> 
> The architecture starts with a **Capability Adapter**. It reads raw CSV logs or API payloads, maps them to a canonical schema using YAML configurations, normalizes values like timestamps, enriches them with historical context, and validates structural integrity.
> 
> Next, the data moves through a **Multi-Agent Diagnostics Pipeline**:
> 1. The **Observer Agent** detects raw metric deviations and trends.
> 2. The **Behavior Agent** analyzes rolling history to detect sustained metric drift.
> 3. The **Risk Prediction Agent** calculates risk scores and classifies risk categories (like SLA breaches or resource exhaustion).
> 4. The **Integrity Agent** checks schema completeness, data quality, and lineage trust.
> 5. The **Recommendation Agent** uses a rule-based selector to fetch SOPs or historical playbooks. For critical incidents, it invokes our **Multi-LLM Selection Agent** which routes the prompt to local Ollama (Llama 3.2 3B), using a robust JSON repair parsing engine to ensure structured recommendations.
> 
> Finally, we have an **Evaluation Layer** that uses RAGAS to compute the factual correctness of our LLM recommendations against Golden Truth scenarios. We also expose a **FastAPI backend** and an interactive **Operations Copilot chat** so engineers can query system state in real time."

---

## 4. 5-Minute Technical Explanation
> "Let's walk through the end-to-end execution flow of the AIF architecture.
> 
> **Data Ingestion and Transformation (Capability Adapter)**:
> In [workflows/main.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/workflows/main.py), we initialize the `CSVConnector` using `config/sources.yaml` to read raw datasets like `airflow_pipeline_runs.csv` and `kafka_stream_metrics.csv`. The `TabularParser` converts these into Python dictionaries. We then pass them to the `MappingEngine` which maps source-specific fields to canonical attributes using mapping profiles in `config/mapping.yaml`. The `Normalizer` formats statuses (e.g. mapping 'ACTIVE' or 'COMPLETED' to 'RUNNING' or 'SUCCESS') and parses timestamps into ISO formats. The `ContextEnricher` merges business metadata, lineage, incident logs, and baselines using memory-efficient lookup indexes. The `ValidationEngine` validates datatype ranges and structural rules, calculating a raw validation score. The `OperationalEntityBuilder` wraps all of this into a Canonical Entity.
> 
> **Agent Diagnostics Layer**:
> - **Observer Agent**: It evaluates the entity using a `BaselineComparator` and `TrendDetector`. It calculates metric deviations (e.g. current elapsed time vs. avg runtime) and maps them to NORMAL, WARNING, or CRITICAL.
> - **Behavior Agent**: Receives observations and computes a Behavior Score and Severity. It uses the `DriftDetector` and `BaselineManager` to check if a warning or critical deviation is sustained over a rolling window (e.g. 3 consecutive abnormal runs).
> - **Risk Prediction Agent**: Uses a `FeatureExtractor` to extract risk inputs (SLA margin ratio, business criticality, deviation severities). The `FeatureNormalizer` normalizes values based on the host platform. The `RiskScoreCalculator` computes a weighted risk score (`Sum(feature * weight) / Sum(weight)`), re-normalizing weights for missing features. The `CategoryClassifier` runs rule-based checks to classify the risk type independently of the score.
> - **Integrity Agent**: Evaluates schema structure, record count statistics, business rules, and lineage completeness. It produces an integrity score (0-100) and maps it to a Trust Level (HIGH, MEDIUM, LOW) using an `IntegrityStatusClassifier`.
> 
> **Action Formulation and Evaluation**:
> - **Recommendation Agent**: Formulates a recommendation. It first builds a deterministic fallback recommendation using rule mappings in `config/recommendation_rules.yaml`. If the entity priority is HIGH or CRITICAL, the `Multi-LLM Selection Agent` attempts to enhance the recommendation using LLMs.
> - **Multi-LLM Selection Agent**: Bypasses external API providers (Gemini, Grok) and routes calls to a local Ollama client running `llama3.2:3b` with a 90-second timeout. It validates and parses the output using an `LLMResponseParser` featuring a three-tiered JSON repair mechanism (regex boundary extraction, literal evaluation using `ast.literal_eval`, and quotes normalization).
> - **Evaluation Service**: Compares the generated recommendation against Golden Truth playbooks in `golden_truth_recommendations.json` via a `ScenarioResolver` utilizing a weighted specificity-matching score. If the Ragas library is installed and a Gemini API key is active, it runs RAGAS to compute `factual_correctness`.
> 
> **API and Copilot Interface**:
> We run FastAPI (`api/main.py`) which exposes `/api/v1/dataops/dashboard` for unified summaries, and `/api/v1/dataops/copilot/chat` for the conversational interface. The `CopilotService` classifies query intents (Greeting, Identity, Help, Small Talk, Operational), extracts pipeline IDs using regex, conjoins dashboard summaries and agent outputs into an operational context, and prompts Ollama to return a structured JSON response containing the diagnosis, root cause, business impact, and suggested actions."

---

## 5. Architecture
```text
  [ Raw Data Ingestion ] (Airflow, Kafka, ADF, Kubernetes, SAP, IoT, Manufacturing CSVs)
             ↓
  [ Capability Adapter ] (Connect, Parse, Map, Normalize, Enrich, Validate, Build Entity)
             ↓
     ( Canonical Entity )
             ↓
     [ Observer Agent ] ----> ( Baseline Comparisons & Trends )
             ↓
     [ Behavior Agent ] ----> ( Severity, Behavior Score & Sustained Drift )
             ↓
   [ Risk Predict Agent ] --> ( Weighted Risk Score & Risk Category )
             ↓
     [ Integrity Agent ] ----> ( Data Quality, Schema, Lineage & Trust Level )
             ↓
   [ Recommendation Agt ] --> ( Fetch SOPs, Prompt Builder, LLM routing )
             ↓
  [ Multi-LLM Selection ] --> ( Call Ollama local daemon, JSON Repair )
             ↓
   [ Evaluation Layer ] ----> ( Golden Truth matching & Ragas correctness )
             ↓
  [ Unified Output JSON ] --> ( FastAPI /api/v1/dataops/dashboard & Copilot Chat )
```

### Component Details
| Component | Purpose | Actual File/Module | Input | Processing | Output | Dependencies |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Capability Adapter** | Load, parse, map, normalize, validate, and enrich raw data into a Canonical Entity. | [adapters/](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/adapters/) | Raw CSV data sources, lookup CSV databases | Config-driven YAML mapping, ISO formatting, index enrichment, datatype range checks | Canonical Entity | `pandas`, `yaml`, `datetime` |
| **Observer Agent** | Capture raw metric deviations, trends, and event correlations. | [agents/observer/](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/observer/) | Canonical Entity | Deviation comparison, trend detection (direction changes), event correlation | Observation Object | `ObserverAgent` sub-components |
| **Behavior Agent** | Analyze rolling history to distinguish transient spikes from sustained behavior drift. | [agents/behavior/](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/behavior/) | Observation Object | Deviation severity classifying, pattern checks, rolling window count | Behavior Object | `BehaviorRuleEngine` |
| **Risk Prediction Agent** | Compute likelihood and impact of operational failures. | [agents/risk/](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/risk/) | Behavior Object | Normalization profile selection, weighted score calculation, rule-based risk categorization | Risk Object | `RiskRuleEngine` |
| **Integrity Agent** | Verify output correctness, schema compliance, and lineage completeness. | [agents/integrity/](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/integrity/) | Observation Object, Canonical Entity | Schema assertion, lineage checks, data quality scores | Integrity Object | `IntegrityRuleEngine` |
| **Recommendation Agent** | Generate action-oriented playbook steps. | [agents/recommendation/](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/recommendation/) | Behavior, Risk, Integrity Objects, Canonical Entity | Priority calculations, SOP retrieval, prompt formatting, validation reverted to fallbacks | Recommendation Object | `MultiLLMSelectionAgent` |
| **Multi-LLM Selection Agent** | Route prompt to available LLM and return valid JSON. | [agents/multi_llm_selection/](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/multi_llm_selection/) | Prompt string, fallback recommendation, entity priority | Provider try-loop (Ollama), robust JSON parsing/repair | Structured JSON Recommendation | `requests`, `ast`, `re` |
| **Evaluation Layer** | Score recommendation correctness against golden truth. | [agents/recommendation/evaluation/](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/recommendation/evaluation/) | Recommendation Object, Context | Specificity matching, Ragas factual correctness call | Evaluated Rec Object | `ragas`, `GoldenTruthRepository` |

---

## 6. Repository Structure
```text
Data-Pipeline-Operations-Intelligence/
├── adapters/
│   ├── connectors/          # Data connectors (csv_connector.py)
│   ├── enricher/            # Joins lookup contexts (context_enricher.py)
│   ├── entity_builder/      # Constructs final canonical model (operational_entity_builder.py)
│   ├── mapping/             # Handles field mappings (mapping_engine.py)
│   ├── normalizer/          # Normalizes status and timestamps (normalizer.py)
│   ├── parsers/             # Converts DataFrames to List[Dict] (tabular_parser.py)
│   └── validation/          # Validates datatypes and ranges (validation_engine.py)
├── agents/
│   ├── base/                # Base agent structures
│   ├── behavior/            # Anomaly, severity, patterns, drift detection
│   ├── integrity/           # Data quality, record counts, schema, lineage check
│   ├── multi_llm_selection/ # Gemini, Grok, Ollama clients, config, and response parser
│   ├── observer/            # Raw metric deviations and trend observers
│   └── recommendation/      # Priority calculators, knowledge retrievers, prompts, evaluation
├── api/
│   ├── main.py              # FastAPI app initialization
│   ├── routes/              # Routes for pipelines, health, copilot chat, dashboard
│   ├── schemas/             # Pydantic request and response models
│   └── services/            # Services for copilot, dashboard summary, timeline, details
├── config/                  # YAML files for rules, mappings, and sources
├── data/
│   ├── evaluation/          # golden_truth_recommendations.json
│   └── raw_sources/         # Source CSV datasets (Airflow, Kafka, SAP, etc.)
├── output/                  # execution_output.json (output registry database)
├── tests/
│   └── unit/                # Unit test suite (copilot, dashboard, LLM selection, evaluation)
└── workflows/
    └── main.py              # Central orchestrator CLI entry point
```

---

## 7. File-by-File Technical Explanation

### File: `adapters/connectors/csv_connector.py`
* **Purpose**: Read source YAML configurations and load source CSV datasets into memory as Pandas DataFrames.
* **Important classes**:
  * `CSVConnector`: Responsible for loading configuration, validating file paths, reading files, and executing local file health checks.
    * *Attributes*: `config_path` (str), `sources` (dict), `connected` (bool).
    * *Methods*: `connect()`, `disconnect()`, `validate_file(file_path)`, `read(source_name)`, `read_all()`, `health_check()`.
* **Important functions**:
  * `read(source_name)`: Takes a source name string, validates that the connector is connected, verifies the target file path exists and is non-empty, and returns `pd.read_csv(file_path)`.
* **Interview explanation**:
  > "This file acts as our ingestion gateway. It reads a YAML source registry listing CSV locations on disk, checks that the files are non-empty and accessible, and loads them into Pandas DataFrames. It isolates raw I/O from the downstream parsing and validation logic."

### File: `adapters/mapping/mapping_engine.py`
* **Purpose**: Map raw columns to canonical variables using configuration-driven mapping profiles.
* **Important classes**:
  * `MappingEngine`: Loads profiles from mapping YAML and maps individual records or datasets.
    * *Attributes*: `mapping_file` (str), `dataset_profiles` (dict), `profiles` (dict).
    * *Methods*: `get_profile(dataset_name)`, `map_record(record, profile)`, `map_dataset(dataset_name, records)`, `map_all(parsed_datasets)`.
* **Important functions**:
  * `map_record(record, profile)`: Evaluates target fields against mapping rules. If `source` is set, it extracts that column; if `const` is set, it assigns a constant; if `default` is set, it assigns a default fallback.
* **Interview explanation**:
  > "This engine standardizes heterogeneous telemetry. Instead of writing separate code parser classes for Airflow, Kafka, or SAP, we represent their field transformations declaratively in `config/mapping.yaml` and resolve them dynamically at run-time."

### File: `adapters/normalizer/normalizer.py`
* **Purpose**: Standardize string values, timestamps, and status values into canonical formats.
* **Important classes**:
  * `Normalizer`: Implements parser methods for normalising string spaces, rounding numbers, formatting ISO dates, and mapping statuses.
* **Important functions**:
  * `normalize_status(status)`: Maps various statuses like 'ACTIVE', 'WAITING', 'COMPLETED', and 'FAIL' to canonical labels like 'RUNNING', 'QUEUED', 'SUCCESS', and 'FAILED'.
  * `normalize_timestamp(value)`: Attempts to parse datetime strings using different formats (`%Y-%m-%dT%H:%M:%S`, `%Y-%m-%d %H:%M:%S`, `%d-%m-%Y %H:%M:%S`) and returns `dt.isoformat()`.
* **Interview explanation**:
  > "Raw data systems represent times and states differently. The Normalizer enforces consistent datatypes: it rounds floats to 2 decimal places, formats date strings into ISO format, and standardizes state enums so downstream agents have clean inputs."

### File: `adapters/enricher/context_enricher.py`
* **Purpose**: Enrich execution records with metadata (business context, lineage relationships, incidents).
* **Important classes**:
  * `ContextEnricher`: Creates dictionaries mapping entity IDs to metadata rows, and injects context blocks.
* **Important functions**:
  * `build_indexes()`: Populates lookup indexes (`business_index`, `history_index`, `lineage_index`) from parsed datasets for fast retrieval.
  * `enrich_record(record)`: Injects nested context blocks (e.g. `record['business_context']`) into the record dictionary.
* **Interview explanation**:
  > "This class enriches raw metrics. It builds in-memory hash index tables from lookup CSVs, and joins the business metadata, incident history, and lineage downstream relationships to the record in O(1) time complexity."

### File: `adapters/validation/validation_engine.py`
* **Purpose**: Validate data records against expected datatypes, allowed status enums, and numerical bounds.
* **Important classes**:
  * `ValidationEngine`: Tracks required fields, datatypes, duplicate status, and calculates validation quality scores.
* **Important functions**:
  * `validate_record(record)`: Evaluates field completeness, datatypes, statuses, ISO timestamps, range limits, business rules, and duplicates.
  * `calculate_score(errors, warnings)`: Starts with 100, subtracts 10 per error, and 2 per warning to compute a final validation score.
* **Interview explanation**:
  > "This validation layer ensures structural data quality. It performs datatype assertions and boundary checks on attributes, tracks duplicate entity IDs, and computes a score that determines whether a record is valid."

### File: `agents/observer/observer_agent.py`
* **Purpose**: Orchestrate raw metrics observation, trend monitoring, and baseline comparison.
* **Important classes**:
  * `ObserverAgent`: Initialises sub-detectors and constructs observation summaries.
* **Important functions**:
  * `observe_entity(entity)`: Runs the state, metric, baseline, trend, and event correlators to build a unified observation block.
* **Interview explanation**:
  > "The Observer Agent is our telemetry scanner. It delegating metrics checks to sub-components to compare execution values to average baselines, detect trend patterns, and flags raw anomalies."

### File: `agents/behavior/behavior_agent.py`
* **Purpose**: Perform historical anomaly analysis and detect sustained performance drift.
* **Important classes**:
  * `BehaviorAgent`: Coordinates rolling history checks, classifies severity, and counts pattern occurrences.
* **Important functions**:
  * `analyze_observation(observation_object)`: Evaluates deviations, sustained drift, and patterns to compute a behavior score and confidence.
* **Interview explanation**:
  > "The Behavior Agent analyzes metrics over time. It uses a rolling history manager to determine if a performance spike is a one-off anomaly or a sustained drift requiring escalation."

### File: `agents/risk/risk_agent.py`
* **Purpose**: Predict operational risk scores, categories, and breach probabilities.
* **Important classes**:
  * `RiskPredictionAgent`: Normalizes risk inputs and calculates probability and severity.
* **Important functions**:
  * `predict_risk(behavior_object)`: Extracts behavior signals, normalizes values by platform profile, calculates risk scores, and routes categorization rules.
* **Interview explanation**:
  > "The Risk Agent predicts incidents. It normalizes features based on the platform, calculates a weighted risk score, and maps it to a risk category independently of the severity."

### File: `agents/integrity/integrity_agent.py`
* **Purpose**: Evaluate schema compliance, lineage trust, and business rules.
* **Important classes**:
  * `IntegrityAgent`: Runs feature extractors, validators, and score classifiers to gauge output trust.
* **Important functions**:
  * `evaluate_integrity(observation_object, operational_entity)`: Validates record counts, schema attributes, lineage context, and business rules.
* **Interview explanation**:
  > "The Integrity Agent checks output quality. It validates data structures, checks upstream/downstream lineage completeness, and outputs a trust score used by the recommendation agent."

### File: `agents/recommendation/recommendation_agent.py`
* **Purpose**: Generate troubleshooting recommendations and orchestrate LLM enhancements.
* **Important classes**:
  * `RecommendationAgent`: Computes priority, retrieves playbooks, builds prompts, and routes requests to Ollama or rule-based fallbacks.
* **Important functions**:
  * `generate_recommendation(...)`: Evaluates context, formats prompts, calls the selection agent, validates output structures, and triggers evaluation.
* **Interview explanation**:
  > "This agent generates playbooks. It retrieves context-matching SOPs and prompts local Ollama (Llama 3.2 3B). If the LLM output fails validation, it falls back to a deterministic recommendation."

### File: `agents/multi_llm_selection/multi_llm_selection_agent.py`
* **Purpose**: Route prompts to active LLM providers and fall back to rules on failure.
* **Important classes**:
  * `MultiLLMSelectionAgent`: Orchestrates the provider try-loop and absorbs exceptions.
* **Important functions**:
  * `select(...)`: Bypasses external providers and routes prompts to Ollama for HIGH/CRITICAL priorities, falling back to rules on errors.
* **Interview explanation**:
  > "This agent routes LLM queries. It handles credentials, handles fallbacks, and executes local inferences. It absorbs API errors to guarantee a recommendation is always returned."

### File: `agents/multi_llm_selection/ollama_client.py`
* **Purpose**: Communicate with the local Ollama API daemon and repair malformed JSON.
* **Important classes**:
  * `OllamaClient`: Handles HTTP requests to the Ollama endpoint and parses JSON.
* **Important functions**:
  * `_parse_response(response_text)`: Extract JSON boundaries, attempts Python literal evaluation, and normalizes quote marks to repair malformed responses.
* **Interview explanation**:
  > "The Ollama Client executes local inference. It sends prompts to the Ollama daemon and includes a parser that repairs malformed JSON syntax to ensure structured outputs."

### File: `api/services/copilot_service.py`
* **Purpose**: Serve conversational requests, conjoin summaries, and handle queries.
* **Important classes**:
  * `CopilotService`: Handles session contexts, classifies intent, and prompts Ollama.
* **Important functions**:
  * `chat(payload)`: Extracts session pipeline IDs, retrieves context summaries, prompts Ollama, and formats conversational replies.
* **Interview explanation**:
  > "The Copilot Service is our chat backend. It tracks session history, joins agent outputs, prompts Ollama, and maps output fields to our API schema."

---

## 8. Class-Level Analysis

### Constructor & Lifecycle Explanations
* **`CSVConnector`**: Configured with a file path. Calling `connect()` loads YAML file maps; calling `read_all()` populates DataFrames.
* **`ContextEnricher`**: Receives lookup datasets in `__init__`. Instantly calls `build_indexes()` to prepare fast retrieval hash maps.
* **`ObserverAgent` & `BehaviorAgent`**: Instantiated with config paths. The orchestrator calls `health_check()` to verify internal status before invoking `observe_all()` or `analyze_all()`.
* **`MultiLLMSelectionAgent`**: Initialises providers (`GeminiClient`, `GrokClient`, `OllamaClient`). During `select()`, it checks configuration state and routes tasks based on priority.

### Design Patterns Used
1. **Dependency Injection**: Used when initializing clients. For example, `GeminiClient` and `OllamaClient` accept an `LLMConfig` dependency in their constructors.
2. **Strategy Pattern**: The `MultiLLMSelectionAgent` selects the model strategy (Ollama vs. rule-based deterministic fallback) based on priority and platform configurations.
3. **Adapter Pattern**: The `Capability Adapter` maps raw CSV source schemas to the Canonical Data Model using declarative YAML rules.
4. **Composite Pattern**: The `ObserverAgent` combines multiple sub-detectors (`StateDetector`, `MetricObserver`, `BaselineComparator`, `TrendDetector`, `EventCorrelator`) to generate a single observation block.
5. **Chain of Responsibility**: Telemetry data passes through a sequential chain of agents (Observer → Behavior → Risk → Integrity → Recommendation) where each agent enriches the payload.

---

## 9. End-to-End Data Flow
```text
Raw CSV Telemetry
       ↓
TabularParser.parse_all()      # DataFrame -> List[Dict], NaNs replaced with None
       ↓
MappingEngine.map_all()        # Replaces raw columns using config/mapping.yaml
       ↓
Normalizer.normalize_all()     # Rounding, timestamp parsing, status formatting
       ↓
ContextEnricher.enrich_all()   # Joins metadata using indexes (business, lineage, etc.)
       ↓
ValidationEngine.validate_all() # Data checks, boundary checks, duplicate validation
       ↓
OperationalEntityBuilder.build() # Cannonical Entity wrapper
       ↓
ObserverAgent.observe_all()    # Baseline comparisons and trend detection
       ↓
BehaviorAgent.analyze_all()    # Deviation severity and sustained drift checks
       ↓
RiskPredictionAgent.predict()  # Normalization profiles, weighted risk score
       ↓
IntegrityAgent.evaluate_all()  # Quality validation, lineage completeness, trust levels
       ↓
RecommendationAgent.generate() # Rule-based action lookup, playbook formatting
       ↓
MultiLLMSelectionAgent.select() # Bypasses API keys, calls Ollama, parses/repairs JSON
       ↓
EvaluationService.evaluate()   # specificity-matching scenario resolver, RAGAS execution
       ↓
JSON Registry File             # output/execution_output.json
       ↓
FastAPI Routes                 # Dashboard aggregation and conversational Copilot chat
```

---

## 10. Capability Adapter
* **Why required**: It decouples our processing logic from raw source schemas. If a platform changes its telemetry format, we modify `config/mapping.yaml` instead of rewriting code.
* **Ingestion Layer**: Ingests raw metrics via the `CSVConnector` and `TabularParser`.
* **Mapping**: Maps raw columns to canonical variables using `MappingEngine`.
* **Normalization**: Enforces uniform formats (dates, states) using `Normalizer`.
* **Enrichment**: Injects business metadata, lineage, and historical incidents using `ContextEnricher`.
* **Validation**: Runs type assertions, range validation, and checks business rules in `ValidationEngine`.
* **Onboarding a new domain**: 
  1. Add the dataset entry to `config/sources.yaml`.
  2. Create a mapping profile in `config/mapping.yaml` matching target fields.
  3. Run the main entry point; the pipeline will parse, normalize, enrich, and build entities automatically.

---

## 11. Canonical Data Model
Our canonical entity represents a unified structure for operational pipeline state:
* `entity_id` (str): Unique identifier.
* `entity_name` (str): Logical pipeline name.
* `entity_type` (str): System asset type (Pipeline, Stream, Batch Job, Pod).
* `source_system` (str): Source platform (Airflow, Kafka, ADF, Kubernetes, SAP, IoT).
* `execution_status` (str): Standardized state (SUCCESS, FAILED, RUNNING, QUEUED, CANCELLED).
* `event_timestamp` (str): ISO format timestamp.
* `attributes` (dict): Platform-specific telemetry (CPU, lag, runtime, throughput).
* `context` (dict): Nesting business context, historical baselines, incidents, and lineage.
* `validation` (dict): Validation scores, errors, and warning messages.
* `metadata` (dict): Version information and generation timestamps.

---

## 12. Pydantic
In `api/schemas/request.py` and `api/schemas/response.py`, Pydantic models enforce API contracts:
* `PipelineDetailsResponse`: Wraps the canonical entity, observation, behavior, risk, integrity, and recommendation models.
* `CopilotChatResponse`: Ensures chat responses return structured fields like `reply`, `rootCause`, `affectedServices`, `businessImpact`, `suggestedActions`, and `confidence`.
* `UnifiedDashboardResponse`: Outlines the dashboard schema, including nodes, anomalies, and agent details.

### Interview QA
* **Why Pydantic?**
  > "Pydantic validates API inputs and structures responses. It handles serialization, deserialization, and type-coercion, and auto-generates OpenAPI documentation."
* **What happens on invalid data?**
  > "FastAPI catches validation errors, halts execution before hitting our service logic, and returns a detailed `422 Unprocessable Entity` response."

---

## 13. FastAPI
* **Initialization**: In `api/main.py`, we initialize `app = FastAPI()` and register dataops, pipeline, and health routers.
* **Async vs Sync Endpoints**: Routes in `routes/dataops.py` are synchronous functions. Because our agent pipeline is CPU-bound, sync functions run in a threadpool to prevent blocking the async event loop.
* **FastAPI Request Lifecycle**:
  ```text
  Client Request → Pydantic Request Parsing → FastAPI Dependency Resolution 
         ↓
  Endpoint Function Execution → Service Layer Call → Pydantic Response Validation 
         ↓
  JSON Serialization → Client Response
  ```

---

## 14. Data Processing
* **Pandas Usage**: Primarily in `CSVConnector` for parsing raw data (`pd.read_csv`). It loads telemetry and metadata into dataframes, which are then converted to native Python dictionary structures for processing.
* **Performance**: We convert raw DataFrames into dictionaries early (`TabularParser`). This avoids the overhead of row-by-row DataFrame operations, resulting in faster execution.

---

## 15. Agents
We implement five active agents, each inheriting from a common base:
1. **Observer Agent**:
   - *File*: [observer_agent.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/observer/observer_agent.py)
   - *Input*: Canonical Entity.
   - *Logic*: Evaluates metric deviations and trends against baseline limits.
   - *Output*: Observation Object.
2. **Behavior Agent**:
   - *File*: [behavior_agent.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/behavior/behavior_agent.py)
   - *Input*: Observation Object.
   - *Logic*: Tracks rolling history to classify severity and identify sustained drift.
   - *Output*: Behavior Object.
3. **Risk Prediction Agent**:
   - *File*: [risk_agent.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/risk/risk_agent.py)
   - *Input*: Behavior Object.
   - *Logic*: Normalizes risk signals by platform and calculates weighted risk scores.
   - *Output*: Risk Object.
4. **Integrity Agent**:
   - *File*: [integrity_agent.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/integrity/integrity_agent.py)
   - *Input*: Observation Object.
   - *Logic*: Validates schema compliance, record counts, and lineage trust.
   - *Output*: Integrity Object.
5. **Recommendation Agent**:
   - *File*: [recommendation_agent.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/recommendation/recommendation_agent.py)
   - *Input*: Behavior, Risk, and Integrity Objects.
   - *Logic*: Forms deterministic recommendations, routes prompts to Ollama, and validates response structures.
   - *Output*: Recommendation Object.

---

## 16. Behavioral Intelligence
* **Deviation Math**:
  `deviation = ((current - baseline) / baseline) * 100`
* **Thresholds**: 
  - Deviation `<= 10%`: NORMAL
  - Deviation `<= 30%`: WARNING
  - Deviation `> 30%`: CRITICAL

---

## 17. Anomaly Detection
* **Mathematical baseline calculations**: We calculate percentage deviations from averages rather than Z-scores. This avoids standard deviation calculations on small datasets and simplifies threshold tuning.

---

## 18. SLA Risk Prediction
* **SLA Drift**: 
  `sla_margin_ratio = (sla_minutes - elapsed_runtime) / sla_minutes`
* **Risk Score Math**: 
  `RiskScore = (Sum(feature * weight) / Sum(weight)) * 100`
  Missing features are omitted, and weights are re-normalized to prevent bias.

---

## 19. Outcome Integrity
* **Calculation**:
  `integrity_score = 100 - (failed_checks * 15) - (warning_checks * 5)`
  Scores map to Trust Levels:
  - Score `>= 90`: HIGH
  - Score `>= 70`: MEDIUM
  - Score `< 70`: LOW

---

## 20. Dependency Intelligence
* **Propagation**: Lineage links pipelines. If an upstream job fails, the downstream validator detects the broken link and lowers the integrity score, propagating the status down the lineage chain.

---

## 21. Simulation
* **Current status**: **Documented but not implemented**. No simulation agent or simulation models exist in this codebase.

---

## 22. Multi-LLM Selection
* **Model Routing**: In `MultiLLMSelectionAgent.select()`, if the priority is HIGH or CRITICAL, the agent routes prompts to local Ollama (Llama 3.2 3B). External clients (Gemini, Grok) are initialized but bypassed in the current loop for stability.

---

## 23. Risk Prediction
* **Prediction Models**: We use a rule-based weighted risk calculation. No machine learning models (e.g. Scikit-learn, XGBoost) are implemented.

---

## 24. Recommendation Engine
* **Context Retrieval**: Retrieves SOPs from `sop_library.json` and platform best practices from `platform_best_practices.json` using string matches.
* **LLM Enhancement**: Formats a prompt, invokes the Multi-LLM Selection Agent, and validates outputs using a `RecommendationValidator` before returning the final recommendation.

---

## 25. LLM Integration
* **Provider**: Local Ollama running Llama 3.2 3B.
* **Endpoints**: API endpoint `http://localhost:11434/api/generate`.
* **JSON Repair**:
  - Outermost brace extraction: `text[first_brace:last_brace+1]`
  - Standard `json.loads`
  - Python literal evaluation via `ast.literal_eval`
  - Single quote replacement with double quotes

---

## 26. Prompt Engineering
* **System Prompt**: Enforces enterprise response guidelines, structures outputs (Summary, Root Cause, Affected Services, Impact, Actions), and demands formatted JSON outputs.
* **Context injection**: Serializes enriched canonical entities, anomalies, and agent states into the prompt string.

---

## 27. Memory & Knowledge Fabric
* **Short-Term Memory**: Conversation history is tracked in `CopilotService._sessions` using a `useCaseId:domainId` key. It retains the last 10 turns of dialogue.
* **Knowledge Retrieval**: Knowledge bases are loaded in `KnowledgeRetriever` to pull SOP guidelines based on matching incident parameters.

---

## 28. Logging
* **Library**: Implemented using Python's standard `logging` library.
* **Configuration**: Configured in [workflows/main.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/workflows/main.py) using `logging.basicConfig(level=LOG_LEVEL, format="%(message)s")`. It reads the `AIF_LOG_LEVEL` environment variable.

---

## 29. Configuration
* **YAML Rules**: Configuration YAML files in `config/` declare mappings, behavior boundaries, risk weights, validation criteria, and recommendation options.
* **Secrets**: Managed via a `.env` file using `python-dotenv`.

---

## 30. Error Handling
* **Client Fallbacks**: The Multi-LLM agent absorbs `PROVIDER_ERRORS` (Gemini, Grok, Ollama connection, validation errors) and falls back to deterministic rule-based recommendations.
* **FastAPI Handlers**: Returns `422` errors on payload validation failures and uses custom exception handlers to prevent service crashes.

---

## 31. Testing
* **Framework**: Built using `pytest`.
* **Unit Tests**:
  - `test_copilot_service.py`: Verifies intent classification and context building.
  - `test_multi_llm_selection.py`: Tests client exceptions, fallbacks, and JSON repair logic.
  - `test_recommendation_evaluation.py`: Validates golden truth resolution and mock RAGAS evaluations.

---

## 32. Security
* **Secrets**: Credentials are loaded from a `.env` file using `load_dotenv()`.
* **Production Improvements Needed**: Add OAuth2/JWT token authentication, restrict API routes with CORS middleware, encrypt secrets, and sanitize inputs to prevent prompt injection.

---

## 33. AI Ethics
* **Traceability**: Decisions are auditable because raw inputs are normalized and stored in a canonical format alongside agent outputs.
* **Hallucination Control**: Prompts instruct Ollama to output information derived only from the provided operational context.

---

## 34. Performance
* **Latency**: The primary bottleneck is the local Ollama LLM execution on CPU.
* **Mitigation**: We set `LIMIT_EVALUATION = True` to evaluate only the representative pipeline during workflows, preventing evaluation bottlenecks.

---

## 35. Scalability
* **Bottlenecks**: Parsing raw CSVs and running LLM inferences sequentially can cause scalability bottlenecks.
* **Mitigation**: Replace file storage with database indexes, process inputs concurrently using asyncio, and cache repetitive LLM prompts.

---

## 36. Deployment
* **Current status**: Local execution via Python scripts. No Dockerfiles or Kubernetes manifests are present in the current implementation.
* **Production Deployment**: Implement Docker containers, run uvicorn workers behind a reverse proxy, and deploy Ollama as a dedicated scalable container service.

---

## 37. Code-Level Questions

### QA 1: What happens when `TabularParser.parse()` encounters a NaN value?
> "It checks for NaNs using `pd.isna(value)` and converts them to Python `None`. This prevents serialisation errors when mapping raw data to JSON."

### QA 2: Why is the `google-generativeai` SDK imported inside `GeminiClient._get_model()` instead of at the top of the file?
> "This implements lazy loading. If the environment lacks the Gemini SDK, the application does not crash on import and can still run using the Grok or Ollama provider strategies."

---

## 38. Scenario-Based Questions

### Scenario 1: A pipeline's duration increases by 150%. How is this resolved?
> "1. The Capability Adapter parses and normalizes the run metrics.
> 2. The Observer Agent compares this runtime to the historical average, calculating a 150% deviation.
> 3. The Behavior Agent flags it as CRITICAL and logs the anomaly.
> 4. The Risk Agent calculates a high risk score and classifies the risk category as 'SLA Breach Risk'.
> 5. The Recommendation Agent retrievals the appropriate recovery steps.
> 6. The Multi-LLM Selection Agent calls Ollama to refine the troubleshooting advice."

### Scenario 2: What happens if the local Ollama daemon is shut down?
> "1. The Ollama request times out or raises a connection error.
> 2. `MultiLLMSelectionAgent` catches `OllamaProviderError`.
> 3. The agent falls back to the deterministic rule-based recommendation.
> 4. The workflow completes successfully without crashing."

---

## 39. Technology "Why" Questions
* **Why Python?**: Standard for data engineering, pandas data processing, and LLM SDK integrations.
* **Why FastAPI?**: Highly performant, supports async, validates schemas using Pydantic, and generates OpenAPI documentation.
* **Why local Ollama?**: Ensures data privacy and offline operational capabilities without external network dependencies.

---

## 40. 100+ Rapid-Fire Questions
*Answers are short and structured for quick review.*

1. **What is AIF?** Adaptive Intelligence Fabric—our operational pipeline diagnostics platform.
2. **What host systems are supported?** Airflow, Kafka, Kubernetes, Azure Data Factory, and SAP ERP.
3. **What is the entry point?** `workflows/main.py`.
4. **How are data loaded?** Using `CSVConnector` and `pandas`.
5. **How is the schema mapped?** Via configuration profiles in `config/mapping.yaml`.
6. **How is state normalized?** Through mapping dictionaries inside `Normalizer.normalize_status()`.
7. **How are timestamps parsed?** In `Normalizer.normalize_timestamp()` using common datetime formats.
8. **What does `ContextEnricher` do?** Integrates business, lineage, and incident metadata into records.
9. **How does the enricher search indexes?** Using in-memory dictionary tables for O(1) lookups.
10. **What validations are run?** Datatype range checks, required fields, and duplicate checks.
11. **How is validation quality scored?** Starts at 100; subtracts 10 per error and 2 per warning.
12. **What does `OperationalEntityBuilder` do?** Packs normalized values into a Canonical Entity.
13. **What is the Observer Agent's role?** Detects metric deviations, trends, and correlated events.
14. **How is deviation calculated?** `((current - baseline) / baseline) * 100`.
15. **What is baseline status criteria?** NORMAL (<=10%), WARNING (<=30%), CRITICAL (>30%).
16. **How is trend detected?** Compares consecutive metrics for increase or decrease.
17. **What is the Behavior Agent's role?** Identifies sustained performance drift and patterns.
18. **How is drift defined?** Having Warning or Critical deviations exceed a rolling count (usually 3).
19. **What is the Risk Agent's role?** Computes risk scores and categories.
20. **Is ML used for risk?** No; it uses a rule-based weighted risk calculation.
21. **How is risk score calculated?** `(Sum(feature * weight) / Sum(weight)) * 100`.
22. **How are missing features handled in risk?** Weights are re-normalized dynamically.
23. **What is the SLA margin ratio?** `(sla_minutes - elapsed_runtime) / sla_minutes`.
24. **How is risk category determined?** Via rules in `risk_rules.yaml` evaluated in `CategoryClassifier`.
25. **What is the Integrity Agent's role?** Validates output correctness and lineage trust.
26. **How is integrity scored?** `100 - (failed_checks * 15) - (warning_checks * 5)`.
27. **What are the output trust levels?** HIGH (>=90), MEDIUM (>=70), and LOW (<70).
28. **Is Simulation implemented?** No, it is documented but not implemented.
29. **What is the Recommendation Agent's role?** Formulates incident playbook steps.
30. **What is fallback recommendation logic?** A deterministic recommendation mapped by priority.
31. **What is the Multi-LLM Selection Agent's role?** Routes prompts to active LLMs.
32. **Which LLM model is used?** Local Ollama running `llama3.2:3b`.
33. **Why are Gemini and Grok bypassed?** To ensure local, offline performance for the demo.
34. **How is LLM response parsed?** Using `LLMResponseParser` to validate required fields.
35. **What JSON repair methods are used?** Regex boundary matching, `ast.literal_eval`, and quotes correction.
36. **What does the Evaluation Service do?** Resolves golden truth scenarios and runs Ragas correctness evaluations.
37. **How does `ScenarioResolver` match scenarios?** Case-insensitive specificity scoring.
38. **Does Ragas evaluate every run?** No, it is limited to the representative pipeline via `LIMIT_EVALUATION = True`.
39. **Where is output registered?** In `output/execution_output.json`.
40. **Is Loguru used?** No, Python's standard `logging` is implemented.
41. **How is log level configured?** Reads `AIF_LOG_LEVEL` environment variable, defaulting to WARNING.
42. **What web framework is used?** FastAPI.
43. **Are endpoints async or sync?** Sync endpoints are threadpool-isolated.
44. **What database is used?** Local JSON registry file registry acting as a database.
45. **How is Copilot intent classified?** Rule-based regex matches against keywords and phrases.
46. **What is the Copilot prompt instruction?** Enterprise Microsoft Copilot style, using only the provided context.
47. **What is the Copilot session key?** `useCaseId:domainId`.
48. **Does Copilot maintain history?** Yes, the last 10 turns of dialogue.
49. **How is Copilot confidence calculated?** Weighted: Risk 40%, Behavior 30%, Integrity 20%, Recommendation 10%.
50. **What is default Copilot timeout?** 90 seconds.
51. **Where are unit tests located?** `tests/unit/`.
52. **How to run tests?** `.venv\Scripts\python.exe -m pytest`.
53. **What is tested in `test_copilot_service`?** Intent classification, context building, and session memory.
54. **What is tested in `test_multi_llm_selection`?** Provider exception handling, fallbacks, and JSON repair.
55. **What is tested in `test_recommendation_evaluation`?** Scenario resolution and Ragas mock evaluations.
56. **Does `TabularParser` handle empty DataFrames?** Yes, it raises a ValueError.
57. **How are YAML files parsed?** Using `yaml.safe_load()`.
58. **Is Docker configured?** No, local virtual environment only.
59. **Is CI/CD implemented?** No, manual verification only.
60. **Is OAuth2 implemented?** No, no API authentication is configured.
61. **Where is representative pipeline ID configured?** `REPRESENTATIVE_PIPELINE_ID` env variable.
62. **What is the representative pipeline ID?** `BRO0001`.
63. **What happens if a mapping profile is missing?** Raises an exception: "No mapping profile for <dataset>".
64. **How are duplicate events avoided in Observer?** `seen_entities` set tracks processed record IDs.
65. **Are floats formatted?** Yes, rounded to 2 decimal places in `Normalizer`.
66. **What happens if `business_context` is missing?** A warning is logged, and an empty dictionary is assigned.
67. **How are business rules evaluated?** Range validations on runtime, retries, and throughput.
68. **What does `verify_keys.py` do?** Checks for configured API keys.
69. **How does `TabularParser` clean strings?** Calling `.strip()` to remove leading/trailing whitespace.
70. **Does `MappingEngine` support constants?** Yes, via the `const` keyword.
71. **Does `MappingEngine` support defaults?** Yes, via the `default` keyword.
72. **What status corresponds to Airflow 'ACTIVE'?** Normalized to 'RUNNING'.
73. **What status corresponds to Airflow 'COMPLETED'?** Normalized to 'SUCCESS'.
74. **What is the default behavior confidence?** 0.8.
75. **What does the Observer detect?** Current metrics and raw deviations.
76. **What does the Behavior Agent detect?** Patterns, behavior scores, and sustained drifts.
77. **Is Grok API key environment variable required?** Yes, `GROK_API_KEY` (if active).
78. **What is default Grok model name?** `grok-4-latest`.
79. **What is default Gemini model name?** `gemini-2.5-flash`.
80. **How does `GeminiClient` configure API key?** Calling `genai.configure(api_key=...)`.
81. **What happens on Gemini 429 errors?** Sets `quota_exceeded = True` to fail fast in subsequent calls.
82. **What is default retry sleep?** 0.5 seconds.
83. **How is `OllamaClient` initialized?** Using `base_url` (http://localhost:11434) and `model` (llama3.2:3b).
84. **What temperature is used for LLMs?** 0.2.
85. **What are the RAGAS metrics?** `factual_correctness`.
86. **Is Ragas evaluator resilient?** Yes, it catches `EvaluationBackendUnavailableError`.
87. **How are scenario match values normalized?** Case-insensitively.
88. **What is the match weight for platform?** 10 (highest priority).
89. **What is the match weight for integrity status?** 3.
90. **What is the match weight for risk severity?** 3.
91. **What is the match weight for behavior severity?** 4.
92. **What is the match weight for failure reason?** 5.
93. **What happens if a resolved scenario has negative match score?** It is skipped.
94. **Where is pipeline execution timeline fetched?** From historical run events.
95. **Are API keys hardcoded?** No, loaded from environment.
96. **What is uvicorn?** The ASGI server running the FastAPI app.
97. **Is Pydantic V2 used?** Yes, standard Pydantic.
98. **How does `CopilotService` handle greetings?** Returns a structured greeting reply directly without calling Ollama.
99. **How does `CopilotService` handle small talk?** Returns conversational small talk directly.
100. **What happens if the CSV is corrupted?** Pandas throws an error; `CSVConnector` raises a ValueError.

---

## 41. Top 30 Difficult Questions

### Q1: Walk me through your code.
> "The pipeline starts in [workflows/main.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/workflows/main.py). In `main()`, we ingest data using `CSVConnector`, parse it with `TabularParser`, map schemas using `MappingEngine`, and run the `Normalizer`. The `ContextEnricher` merges metadata, the `ValidationEngine` checks values, and `OperationalEntityBuilder` builds the Canonical Entity.
> Next, the `ObserverAgent` calculates deviations. The `BehaviorAgent` detects rolling drifts, the `RiskPredictionAgent` calculates risk scores, the `IntegrityAgent` evaluates trust levels, and the `RecommendationAgent` retrieves playbooks, routing queries to Ollama before evaluating correctness."

### Q2: Show me where the LLM is integrated.
> "The LLM is integrated in [agents/multi_llm_selection/ollama_client.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/multi_llm_selection/ollama_client.py) inside the `generate()` method, which calls a local Ollama server running Llama 3.2 3B. It is also integrated in [api/services/copilot_service.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/api/services/copilot_service.py) in the `chat()` method to generate structured responses."

### Q3: How exactly does your Behavior Agent detect sustained drift?
> "In [agents/behavior/drift_detector.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/behavior/drift_detector.py), the `detect()` method records metrics in a `BaselineManager`. It counts how many recent entries have a warning or critical severity. If the count matches or exceeds the threshold, `is_drifting` is set to True."

### Q4: How do you calculate risk?
> "In [agents/risk/risk_score_calculator.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/risk/risk_score_calculator.py), we calculate the weighted risk score: `WeightedSum / TotalWeight * 100`. Missing features are omitted, and weights are re-normalized to avoid score bias."

### Q5: How do you calculate anomaly severity?
> "In [agents/observer/baseline_comparator.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/observer/baseline_comparator.py), the `get_status()` method checks deviation: deviation `<= 10%` is NORMAL, `<= 30%` is WARNING, and `> 30%` is CRITICAL."

### Q6: How does the Capability Adapter standardise schemas?
> "In [adapters/mapping/mapping_engine.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/adapters/mapping/mapping_engine.py), the mapping engine parses input fields to target canonical fields based on the YAML configuration file (`config/mapping.yaml`)."

### Q7: What happens if the LLM fails?
> "The `MultiLLMSelectionAgent` catches `PROVIDER_ERRORS` and returns the deterministic rule-based fallback recommendation, preventing execution failures."

### Q8: How do you validate LLM output?
> "In [agents/recommendation/recommendation_validator.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/recommendation/recommendation_validator.py), the validator checks that the LLM response is a dictionary containing required fields, correct types, and matched priorities."

### Q9: How do you log requests?
> "We configure Python's standard `logging` library in `workflows/main.py`. The root logger level is read from the `AIF_LOG_LEVEL` environment variable."

### Q10: How do you make the system domain agnostic?
> "All transformations are defined in mapping and validation YAML files. To support a new domain, we add a new configuration profile without modifying any code."

### Q11: How is the Golden Truth resolved?
> "In [agents/recommendation/evaluation/scenario_resolver.py](file:///c:/Users/Asus/OneDrive/Documents/Desktop/Data-Pipeline-Operations-Intelligence/agents/recommendation/evaluation/scenario_resolver.py), we run a specificity-matching algorithm that calculates match scores based on platform, execution status, and anomalies."

### Q12: Why are Gemini and Grok clients implemented but bypassed?
> "They are implemented to support cloud deployments, but bypassed in the current loop to ensure local demo execution without external network dependencies."

### Q13: What happens on ADF runtime milliseconds conversion?
> "We note it as a known limitation: ADF duration is in milliseconds while other platforms use seconds. The metrics are normalized based on platform profiles."

### Q14: How does Copilot Service track session memory?
> "It stores a list of dictionaries tracking user/assistant turns in `CopilotService._sessions` keyed by `useCaseId:domainId`."

### Q15: Why is `ast.literal_eval` used in JSON repair?
> "If Ollama outputs valid Python dictionary syntax (using single quotes and True/False/None) instead of JSON, standard JSON parsers fail. `ast.literal_eval` parses it correctly."

### Q16: How do you handle database persistence?
> "We store state in a local JSON registry file `output/execution_output.json`. This acts as our operational database, which the FastAPI services query."

### Q17: What is the primary bottleneck in the system?
> "Running local LLM inference on CPU is the primary bottleneck. We address this by running evaluations only for the representative pipeline."

### Q18: How does the category classifier work?
> "It evaluates a flat dictionary of signals against rules in `risk_rules.yaml` using operators like min, max, equals, and contains, independent of the risk score."

### Q19: Why not use a database like Postgres?
> "A local JSON file was chosen to simplify demo deployment. In production, we would replace this with a PostgreSQL database."

### Q20: How are warnings handled in quality scores?
> "Errors subtract 10 points and warnings subtract 2 points from the starting score of 100 in `ValidationEngine`."

### Q21: What happens if a CSV file is empty?
> "`CSVConnector.validate_file()` raises a `ValueError('Empty file')`, halting execution."

### Q22: What happens if the mapping YAML contains an invalid profile?
> "The mapping engine raises a KeyError, which is caught and logged as an error."

### Q23: Why do we normalize state enums?
> "Different systems use different state representations (e.g. active, waiting, fail). Normalizing them ensures downstream agents can run consistent evaluations."

### Q24: How does RAGAS evaluation run?
> "It calls `evaluate_correctness()` in `RagasEvaluator` to score the generated recommendation against the Golden Truth expected response."

### Q25: How do we handle threadpool isolation in FastAPI?
> "Since our routes are defined as standard synchronous `def` functions, FastAPI automatically runs them in an external threadpool to prevent blocking."

### Q26: Why is `LIMIT_EVALUATION` configured?
> "RAGAS evaluations require LLM calls. Setting `LIMIT_EVALUATION` to True ensures we evaluate only the representative pipeline, avoiding rate limits."

### Q27: How does the Context Builder conjoin data sources?
> "It aggregates dashboard data and pipeline details into a single Pydantic-compliant context structure for Copilot queries."

### Q28: How is the risk severity classified?
> "Calculated from the risk score: `<= 25` is LOW, `<= 50` is MEDIUM, `<= 75` is HIGH, and `> 75` is CRITICAL."

### Q29: What happens on duplicate entity IDs?
> "The `ValidationEngine` tracks processed entity IDs. If it detects a duplicate, it logs a validation error and lowers the quality score."

### Q30: How would you secure the API in production?
> "I would implement JWT authentication, set up CORS middleware, and use a secrets manager (e.g. AWS Secrets Manager) instead of `.env` files."

---

## 42. Potential Weak Areas & Interview Traps

### Traps & Weaknesses
1. **Slow Unit Tests**:
   - *Issue*: `test_copilot_service.py` is slow because it calls `CopilotService.chat()`, which attempts to connect to `localhost:11434`.
   - *Why it's a trap*: The interviewer might ask why unit tests require a running local Ollama service.
   - *Answer*: This is a known testing gap; we should mock the network requests to Ollama in our test suite.
2. **Hardcoded Ollama Provider**:
   - *Issue*: `MultiLLMSelectionAgent.select()` contains a hardcoded provider list: `for provider_name in ["ollama"]`.
   - *Why it's a trap*: The interviewer might point out that cloud LLMs (Gemini, Grok) are never reached.
   - *Answer*: This was hardcoded to ensure demo reliability on local environments. In production, this list would be read dynamically from `llm_config.get_selection_order()`.
3. **Loguru Mentioned but Standard Logging Used**:
   - *Issue*: Documentation mentions Loguru, but the codebase uses standard Python `logging`.
   - *Answer*: The initial design proposed Loguru, but we implemented the standard library to minimize external dependencies.
4. **No Real Database**:
   - *Issue*: The system reads and writes to local JSON registry files (`output/execution_output.json`).
   - *Answer*: Using local files simplified the demo architecture. In production, we would integrate PostgreSQL.

---

## 43. Production Improvements
1. **Mock HTTP Requests in Tests**: Use `responses` or `pytest-mock` to mock calls to Ollama.
2. **Dynamic LLM Routing**: Restore dynamic routing by reading provider order from configurations.
3. **Database Integration**: Replace local JSON registries with a PostgreSQL database.
4. **API Authentication**: Add OAuth2 and JWT tokens to secure endpoints.
5. **Observability Monitoring**: Integrate Prometheus and Grafana to track agent latencies and request rates.

---

## 44. Final Interview Cheat Sheet

### Project in 10 lines
* **Name**: Adaptive Intelligence Fabric (AIF).
* **Heterogeneous Telemetry**: Ingests Airflow DAGs, Kafka metrics, ADF runs, Kubernetes pods, and SAP jobs.
* **Capability Adapter**: YAML-driven schema mapping, normalization, enrichment, and validation.
* **Orchestration**: Runs sequential agents to detect anomalies and predict risks.
* **Local Inference**: Uses a local Ollama daemon running Llama 3.2 3B for troubleshooting recommendations.
* **Robust Parser**: Features a JSON parser to extract and repair malformed outputs.
* **Evaluation**: Uses a Scenario Resolver and RAGAS to score recommendations against Golden Truth data.
* **Copilot Chat**: Conversational interface for operational queries.
* **FastAPI**: Serves unified dashboard data.
* **Registry Database**: Stores state in `execution_output.json`.

### Every Agent in One Line
* **Observer Agent**: Detects metric deviations and trends.
* **Behavior Agent**: Analyzes rolling history to detect sustained drifts.
* **Risk Agent**: Computes platform-normalized risk scores and categories.
* **Integrity Agent**: Evaluates schema compliance, record counts, and lineage trust.
* **Recommendation Agent**: Formulates playbook recommendations and routes LLM calls.

### LLM integration in 5 lines
* Communicates with local Ollama (`llama3.2:3b`) on `http://localhost:11434`.
* Formatted JSON system prompts enforce structured responses.
* JSON parser extracts responses using bracket match boundaries.
* Repairs syntax using `ast.literal_eval` and quote normalization.
* Catches connection errors to fall back to deterministic recommendations.

### Pydantic & FastAPI in 3 lines
* FastAPI serves routes for dashboards, timeline events, and Copilot chats.
* Pydantic schemas validate API requests and structure JSON responses.
* CPU-bound endpoints run in threadpools to avoid blocking execution.

### Anomaly & Risk in 3 lines
* Anomaly calculated as percentage deviation: `((current - baseline) / baseline) * 100`.
* Risk score calculated as weighted sum: `(Sum(feature * weight) / Sum(weight)) * 100`.
* Risk categories are classified using YAML-defined rules.
