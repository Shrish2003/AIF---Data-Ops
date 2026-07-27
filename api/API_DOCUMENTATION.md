# Data Pipeline Operations Intelligence

> All examples shown in this document are real responses captured from the running backend. They are intended to help frontend integration and API validation. Values will change dynamically depending on workflow execution.

## API Overview

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| GET | `/health` | Health Check |
| POST | `/pipeline/execute` | Execute the complete AIF pipeline. |
| POST | `/api/v1/dataops/execute` | Execute complete AIF pipeline workflow. |
| GET | `/api/v1/dataops/dashboard` | Retrieve and aggregate summaries for the unified dashboard. |
| GET | `/api/v1/dataops/pipelines/{pipelineId}` | Retrieve all AIF layers (operational, observation, behavior, risk, integrity, recommendation) for a pipeline. |
| GET | `/api/v1/dataops/pipelines/{pipelineId}/timeline` | Retrieve or construct execution history timestamps for the selected pipeline. |
| GET | `/api/v1/dataops/pipelines/{pipelineId}/dependencies` | Retrieve lineage, upstream, and downstream dependencies using historical context datasets. |
| GET | `/api/v1/dataops/recommendations/{pipelineId}` | Retrieve recommendation assessment metadata for the selected pipeline. |
| POST | `/api/v1/dataops/copilot/chat` | Interface with the Operations Copilot backend to ask questions about pipeline executions. |
| GET | `/api/v1/dataops/agents` | Retrieve operational health, status, and versions of the AIF Agents. |

---

# Health Check API

## Endpoint

GET /health

## Purpose

Health Check

## Request

GET /health

## Path Parameters

None

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "status": "healthy",
  "service": "Adaptive Intelligence Fabric API",
  "version": "1.0.0"
}
```

## Status Codes

200 OK

---

# Execute Pipeline API

## Endpoint

POST /pipeline/execute

## Purpose

Execute the complete AIF pipeline.

## Request

POST /pipeline/execute

## Path Parameters

None

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "status": "SUCCESS",
  "execution_time": 106.63,
  "output_file": "output\\execution_output.json",
  "adapter_summary": {
    "data_sources_loaded": 12,
    "raw_datasets": 7,
    "lookup_datasets": 5,
    "parsed_raw_records": 7000,
    "parsed_lookup_records": 35000,
    "mapped_records": 7000,
    "normalized_records": 7000,
    "validated_records": 7000,
    "operational_entities": 7000
  },
  "observer_summary": {
    "operational_entities": 7000,
    "observation_objects": 7000
  },
  "behavior_summary": {
    "observation_objects": 7000,
    "behavior_objects": 7000
  },
  "risk_summary": {
    "behavior_objects": 7000,
    "risk_objects": 7000
  },
  "integrity_summary": {
    "observation_objects": 7000,
    "integrity_objects": 7000
  },
  "recommendation_summary": {
    "behavior_objects": 7000,
    "recommendation_objects": 3349,
    "generated_via_llm": 1,
    "generated_via_deterministic": 3348
  }
}
```

## Status Codes

200 OK

---

# Execute DataOps Pipeline API

## Endpoint

POST /api/v1/dataops/execute

## Purpose

Execute complete AIF pipeline workflow.

## Request

POST /api/v1/dataops/execute

## Path Parameters

None

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "status": "SUCCESS",
  "execution_time": 106.63,
  "output_file": "output\\execution_output.json",
  "adapter_summary": {
    "data_sources_loaded": 12,
    "raw_datasets": 7,
    "lookup_datasets": 5,
    "parsed_raw_records": 7000,
    "parsed_lookup_records": 35000,
    "mapped_records": 7000,
    "normalized_records": 7000,
    "validated_records": 7000,
    "operational_entities": 7000
  },
  "observer_summary": {
    "operational_entities": 7000,
    "observation_objects": 7000
  },
  "behavior_summary": {
    "observation_objects": 7000,
    "behavior_objects": 7000
  },
  "risk_summary": {
    "behavior_objects": 7000,
    "risk_objects": 7000
  },
  "integrity_summary": {
    "observation_objects": 7000,
    "integrity_objects": 7000
  },
  "recommendation_summary": {
    "behavior_objects": 7000,
    "recommendation_objects": 3349,
    "generated_via_llm": 1,
    "generated_via_deterministic": 3348
  }
}
```

## Status Codes

200 OK

---

# Dashboard API

## Endpoint

GET /api/v1/dataops/dashboard

## Purpose

Retrieve and aggregate summaries for the unified dashboard.

## Request

GET /api/v1/dataops/dashboard

## Path Parameters

None

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "useCaseId": "data-ops",
  "useCaseName": "Data Pipeline Operations Intelligence",
  "timestamp": "2026-07-27T19:45:29.233211Z",
  "summary": {
    "overallHealth": 67,
    "overallStatus": "critical",
    "riskLevel": "medium",
    "activeAnomalies": 12,
    "criticalAnomalies": 12,
    "executionStatus": "SUCCESS",
    "lastExecutionTime": "2026-07-27T19:45:29.233211Z"
  },
  "health": {
    "nodes": [
      {
        "id": "BRO0001",
        "label": "asset.maintenance.alerts",
        "score": 50,
        "status": "critical",
        "platform": "Kafka",
        "lastExecution": "2026-06-25T00:40:00",
        "pipelineId": "BRO0001",
        "pipelineName": "asset.maintenance.alerts",
        "healthScore": 50
      },
      {
        "id": "BRO0002",
        "label": "customer.profile.updates",
        "score": 50,
        "status": "critical",
        "platform": "Kafka",
        "lastExecution": "2026-07-25T18:41:00",
        "pipelineId": "BRO0002",
        "pipelineName": "customer.profile.updates",
        "healthScore": 50
      },
      {
        "id": "RUN000001",
        "label": "billing_pipeline",
        "score": 76,
        "status": "healthy",
        "platform": "Airflow",
        "lastExecution": "2026-07-12T00:19:00",
        "pipelineId": "RUN000001",
        "pipelineName": "billing_pipeline",
        "healthScore": 76
      },
      {
        "id": "RUN000002",
        "label": "asset_management_etl",
        "score": 76,
        "status": "healthy",
        "platform": "Airflow",
        "lastExecution": "2026-06-19T15:17:00",
        "pipelineId": "RUN000002",
        "pipelineName": "asset_management_etl",
        "healthScore": 76
      },
      {
        "id": "billing-worker-c49885",
        "label": "billing-worker-c49885",
        "score": 76,
        "status": "healthy",
        "platform": "Kubernetes",
        "lastExecution": "2026-06-09T13:34:00",
        "pipelineId": "billing-worker-c49885",
        "pipelineName": "billing-worker-c49885",
        "healthScore": 76
      },
      {
        "id": "order-processor-d8106f",
        "label": "order-processor-d8106f",
        "score": 76,
        "status": "healthy",
        "platform": "Kubernetes",
        "lastExecution": "2026-07-18T08:46:00",
        "pipelineId": "order-processor-d8106f",
        "pipelineName": "order-processor-d8106f",
        "healthScore": 76
      },
      {
        "id": "PIP000001",
        "label": "RevenueRecognitionETL",
        "score": 62,
        "status": "healthy",
        "platform": "Azure Data Factory",
        "lastExecution": "None",
        "pipelineId": "PIP000001",
        "pipelineName": "RevenueRecognitionETL",
        "healthScore": 62
      },
      {
        "id": "PIP000002",
        "label": "EmployeeEngagementETL",
        "score": 65,
        "status": "healthy",
        "platform": "Azure Data Factory",
        "lastExecution": "None",
        "pipelineId": "PIP000002",
        "pipelineName": "EmployeeEngagementETL",
        "healthScore": 65
      },
      {
        "id": "19919034",
        "label": "GL Account Reconciliation",
        "score": 70,
        "status": "healthy",
        "platform": "SAP ERP",
        "lastExecution": "2026-06-08T15:02:00",
        "pipelineId": "19919034",
        "pipelineName": "GL Account Reconciliation",
        "healthScore": 70
      },
      {
        "id": "17025855",
        "label": "Tax Calculation Run",
        "score": 70,
        "status": "healthy",
        "platform": "SAP ERP",
        "lastExecution": "2026-06-08T07:52:00",
        "pipelineId": "17025855",
        "pipelineName": "Tax Calculation Run",
        "healthScore": 70
      }
    ]
  },
  "anomalies": [
    {
      "id": "anom-BRO0001-0",
      "sev": "P2",
      "severity": "P2",
      "time": "00:40",
      "service": "asset.maintenance.alerts",
      "pipeline": "asset.maintenance.alerts",
      "signal": "Throughput deviation (+21.4%)",
      "agent": "Behavior Agent"
    },
    {
      "id": "anom-BRO0001-1",
      "sev": "P1",
      "severity": "P1",
      "time": "00:40",
      "service": "asset.maintenance.alerts",
      "pipeline": "asset.maintenance.alerts",
      "signal": "Throughput deviation (+41.2%)",
      "agent": "Behavior Agent"
    },
    {
      "id": "anom-BRO0002-2",
      "sev": "P1",
      "severity": "P1",
      "time": "18:41",
      "service": "customer.profile.updates",
      "pipeline": "customer.profile.updates",
      "signal": "Throughput deviation (+140.0%)",
      "agent": "Behavior Agent"
    },
    {
      "id": "anom-BRO0007-3",
      "sev": "P1",
      "severity": "P1",
      "time": "08:21",
      "service": "campaign.performance.metrics",
      "pipeline": "campaign.performance.metrics",
      "signal": "Throughput deviation (-99.4%)",
      "agent": "Behavior Agent"
    },
    {
      "id": "anom-BRO0004-4",
      "sev": "P1",
      "severity": "P1",
      "time": "12:26",
      "service": "compliance.kyc.checks",
      "pipeline": "compliance.kyc.checks",
      "signal": "Throughput deviation (-99.7%)",
      "agent": "Behavior Agent"
    },
    {
      "id": "anom-BRO0011-5",
      "sev": "P1",
      "severity": "P1",
      "time": "09:05",
      "service": "churn.risk.updates",
      "pipeline": "churn.risk.updates",
      "signal": "Throughput deviation (-81.4%)",
      "agent": "Behavior Agent"
    }
  ],
  "agents": [
    {
      "id": "CAP",
      "displayName": "Capability Adapter",
      "status": "UP",
      "signals": 5000,
      "signalsProcessed": 5000,
      "health": "OK"
    },
    {
      "id": "OBS",
      "displayName": "Observer Agent",
      "status": "UP",
      "signals": 5000,
      "signalsProcessed": 5000,
      "health": "OK"
    },
    {
      "id": "BEH",
      "displayName": "Behavior Agent",
      "status": "UP",
      "signals": 5000,
      "signalsProcessed": 5000,
      "health": "OK"
    },
    {
      "id": "RISK",
      "displayName": "Risk Prediction Agent",
      "status": "UP",
      "signals": 5000,
      "signalsProcessed": 5000,
      "health": "OK"
    },
    {
      "id": "INT",
      "displayName": "Integrity Agent",
      "status": "UP",
      "signals": 5000,
      "signalsProcessed": 5000,
      "health": "OK"
    },
    {
      "id": "REC",
      "displayName": "Recommendation Agent",
      "status": "UP",
      "signals": 3209,
      "signalsProcessed": 3209,
      "health": "OK"
    }
  ],
  "riskSummary": {
    "servicesAtRisk": 1024,
    "criticalPipelines": 923,
    "cascadeProbability": 69,
    "monitoringCoverage": 1,
    "agentSignals": 12
  },
  "copilot": {
    "summary": {
      "overallHealth": 67,
      "overallStatus": "critical",
      "riskLevel": "medium",
      "activeAnomalies": 12,
      "criticalAnomalies": 12,
      "executionStatus": "SUCCESS",
      "lastExecutionTime": "2026-07-27T19:45:29.233211Z"
    },
    "anomalies": [
      {
        "id": "anom-BRO0001-0",
        "sev": "P2",
        "severity": "P2",
        "time": "00:40",
        "service": "asset.maintenance.alerts",
        "pipeline": "asset.maintenance.alerts",
        "signal": "Throughput deviation (+21.4%)",
        "agent": "Behavior Agent"
      },
      {
        "id": "anom-BRO0001-1",
        "sev": "P1",
        "severity": "P1",
        "time": "00:40",
        "service": "asset.maintenance.alerts",
        "pipeline": "asset.maintenance.alerts",
        "signal": "Throughput deviation (+41.2%)",
        "agent": "Behavior Agent"
      },
      {
        "id": "anom-BRO0002-2",
        "sev": "P1",
        "severity": "P1",
        "time": "18:41",
        "service": "customer.profile.updates",
        "pipeline": "customer.profile.updates",
        "signal": "Throughput deviation (+140.0%)",
        "agent": "Behavior Agent"
      },
      {
        "id": "anom-BRO0007-3",
        "sev": "P1",
        "severity": "P1",
        "time": "08:21",
        "service": "campaign.performance.metrics",
        "pipeline": "campaign.performance.metrics",
        "signal": "Throughput deviation (-99.4%)",
        "agent": "Behavior Agent"
      },
      {
        "id": "anom-BRO0004-4",
        "sev": "P1",
        "severity": "P1",
        "time": "12:26",
        "service": "compliance.kyc.checks",
        "pipeline": "compliance.kyc.checks",
        "signal": "Throughput deviation (-99.7%)",
        "agent": "Behavior Agent"
      },
      {
        "id": "anom-BRO0011-5",
        "sev": "P1",
        "severity": "P1",
        "time": "09:05",
        "service": "churn.risk.updates",
        "pipeline": "churn.risk.updates",
        "signal": "Throughput deviation (-81.4%)",
        "agent": "Behavior Agent"
      }
    ],
    "health": {
      "nodes": [
        {
          "id": "BRO0001",
          "label": "asset.maintenance.alerts",
          "score": 50,
          "status": "critical",
          "platform": "Kafka",
          "lastExecution": "2026-06-25T00:40:00",
          "pipelineId": "BRO0001",
          "pipelineName": "asset.maintenance.alerts",
          "healthScore": 50
        },
        {
          "id": "BRO0002",
          "label": "customer.profile.updates",
          "score": 50,
          "status": "critical",
          "platform": "Kafka",
          "lastExecution": "2026-07-25T18:41:00",
          "pipelineId": "BRO0002",
          "pipelineName": "customer.profile.updates",
          "healthScore": 50
        },
        {
          "id": "RUN000001",
          "label": "billing_pipeline",
          "score": 76,
          "status": "healthy",
          "platform": "Airflow",
          "lastExecution": "2026-07-12T00:19:00",
          "pipelineId": "RUN000001",
          "pipelineName": "billing_pipeline",
          "healthScore": 76
        },
        {
          "id": "RUN000002",
          "label": "asset_management_etl",
          "score": 76,
          "status": "healthy",
          "platform": "Airflow",
          "lastExecution": "2026-06-19T15:17:00",
          "pipelineId": "RUN000002",
          "pipelineName": "asset_management_etl",
          "healthScore": 76
        },
        {
          "id": "billing-worker-c49885",
          "label": "billing-worker-c49885",
          "score": 76,
          "status": "healthy",
          "platform": "Kubernetes",
          "lastExecution": "2026-06-09T13:34:00",
          "pipelineId": "billing-worker-c49885",
          "pipelineName": "billing-worker-c49885",
          "healthScore": 76
        },
        {
          "id": "order-processor-d8106f",
          "label": "order-processor-d8106f",
          "score": 76,
          "status": "healthy",
          "platform": "Kubernetes",
          "lastExecution": "2026-07-18T08:46:00",
          "pipelineId": "order-processor-d8106f",
          "pipelineName": "order-processor-d8106f",
          "healthScore": 76
        },
        {
          "id": "PIP000001",
          "label": "RevenueRecognitionETL",
          "score": 62,
          "status": "healthy",
          "platform": "Azure Data Factory",
          "lastExecution": "None",
          "pipelineId": "PIP000001",
          "pipelineName": "RevenueRecognitionETL",
          "healthScore": 62
        },
        {
          "id": "PIP000002",
          "label": "EmployeeEngagementETL",
          "score": 65,
          "status": "healthy",
          "platform": "Azure Data Factory",
          "lastExecution": "None",
          "pipelineId": "PIP000002",
          "pipelineName": "EmployeeEngagementETL",
          "healthScore": 65
        },
        {
          "id": "19919034",
          "label": "GL Account Reconciliation",
          "score": 70,
          "status": "healthy",
          "platform": "SAP ERP",
          "lastExecution": "2026-06-08T15:02:00",
          "pipelineId": "19919034",
          "pipelineName": "GL Account Reconciliation",
          "healthScore": 70
        },
        {
          "id": "17025855",
          "label": "Tax Calculation Run",
          "score": 70,
          "status": "healthy",
          "platform": "SAP ERP",
          "lastExecution": "2026-06-08T07:52:00",
          "pipelineId": "17025855",
          "pipelineName": "Tax Calculation Run",
          "healthScore": 70
        }
      ]
    },
    "agents": [
      {
        "id": "CAP",
        "displayName": "Capability Adapter",
        "status": "UP",
        "signals": 5000,
        "signalsProcessed": 5000,
        "health": "OK"
      },
      {
        "id": "OBS",
        "displayName": "Observer Agent",
        "status": "UP",
        "signals": 5000,
        "signalsProcessed": 5000,
        "health": "OK"
      },
      {
        "id": "BEH",
        "displayName": "Behavior Agent",
        "status": "UP",
        "signals": 5000,
        "signalsProcessed": 5000,
        "health": "OK"
      },
      {
        "id": "RISK",
        "displayName": "Risk Prediction Agent",
        "status": "UP",
        "signals": 5000,
        "signalsProcessed": 5000,
        "health": "OK"
      },
      {
        "id": "INT",
        "displayName": "Integrity Agent",
        "status": "UP",
        "signals": 5000,
        "signalsProcessed": 5000,
        "health": "OK"
      },
      {
        "id": "REC",
        "displayName": "Recommendation Agent",
        "status": "UP",
        "signals": 3209,
        "signalsProcessed": 3209,
        "health": "OK"
      }
    ]
  }
}
```

## Status Codes

200 OK

---

# Pipeline Details API

## Endpoint

GET /api/v1/dataops/pipelines/{pipelineId}

## Purpose

Retrieve all AIF layers (operational, observation, behavior, risk, integrity, recommendation) for a pipeline.

## Request

GET /api/v1/dataops/pipelines/{pipelineId}

## Path Parameters

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `pipelineId` | string | Yes | The ID of the pipeline (e.g. BRO0001) |

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "operational_entity": {
    "entity_id": "BRO0001",
    "entity_name": "asset.maintenance.alerts",
    "entity_type": "Stream",
    "source_system": "Kafka",
    "execution_status": "FAILED",
    "event_timestamp": "2026-06-25T00:40:00",
    "attributes": {
      "consumer_group": "order-processing-service",
      "partition": 20,
      "offset": 38004142,
      "consumer_lag": 57,
      "throughput": 607,
      "severity": "INFO"
    },
    "context": {
      "business_context": {
        "entity_id": "BRO0001",
        "source_system": "Kafka",
        "business_unit": "Human Resources",
        "owner": "Michael Brown",
        "criticality": "High",
        "sla_minutes": 5,
        "application": "Fraud Detection",
        "cost_center": "HR-201"
      },
      "historical_context": {
        "entity_id": "BRO0001",
        "source_system": "Kafka",
        "avg_runtime_sec": 30,
        "success_rate": 96.39,
        "avg_cpu": 62.0,
        "avg_memory": 57.23,
        "avg_throughput": 500
      },
      "incident_context": {
        "entity_id": "BRO0001",
        "source_system": "Kafka",
        "total_incidents": 9,
        "last_incident": "2026-07-24",
        "root_cause": "Pod Crash",
        "resolution": "Replace Sensor",
        "severity": "Critical"
      },
      "lineage_context": {
        "entity_id": "BRO0001",
        "source_system": "Kafka",
        "depends_on": "DAG0093",
        "downstream": "BRO0001",
        "dependency_type": "Processes"
      },
      "recommendation_context": {
        "entity_id": "BRO0001",
        "source_system": "Kafka",
        "recommendation": "Scale Kubernetes Cluster",
        "impact": "Improved Reliability",
        "applied": "Yes",
        "success_rate": 95
      }
    },
    "validation": {
      "validation_score": 100,
      "is_valid": true,
      "errors": [],
      "warnings": []
    },
    "metadata": {
      "adapter_version": "1.0",
      "pipeline_stage": "Capability Adapter",
      "generated_at": "2026-07-27T19:43:10.945234"
    }
  },
  "observation_object": {
    "entity_id": "BRO0001",
    "entity_name": "asset.maintenance.alerts",
    "entity_type": "Stream",
    "source_system": "Kafka",
    "event_timestamp": "2026-06-25T00:40:00",
    "entity": {
      "entity_id": "BRO0001",
      "entity_name": "asset.maintenance.alerts",
      "entity_type": "Stream",
      "source_system": "Kafka",
      "execution_status": "FAILED",
      "event_timestamp": "2026-06-25T00:40:00",
      "attributes": {
        "consumer_group": "order-processing-service",
        "partition": 20,
        "offset": 38004142,
        "consumer_lag": 57,
        "throughput": 607,
        "severity": "INFO"
      },
      "context": {
        "business_context": {
          "entity_id": "BRO0001",
          "source_system": "Kafka",
          "business_unit": "Human Resources",
          "owner": "Michael Brown",
          "criticality": "High",
          "sla_minutes": 5,
          "application": "Fraud Detection",
          "cost_center": "HR-201"
        },
        "historical_context": {
          "entity_id": "BRO0001",
          "source_system": "Kafka",
          "avg_runtime_sec": 30,
          "success_rate": 96.39,
          "avg_cpu": 62.0,
          "avg_memory": 57.23,
          "avg_throughput": 500
        },
        "incident_context": {
          "entity_id": "BRO0001",
          "source_system": "Kafka",
          "total_incidents": 9,
          "last_incident": "2026-07-24",
          "root_cause": "Pod Crash",
          "resolution": "Replace Sensor",
          "severity": "Critical"
        },
        "lineage_context": {
          "entity_id": "BRO0001",
          "source_system": "Kafka",
          "depends_on": "DAG0093",
          "downstream": "BRO0001",
          "dependency_type": "Processes"
        },
        "recommendation_context": {
          "entity_id": "BRO0001",
          "source_system": "Kafka",
          "recommendation": "Scale Kubernetes Cluster",
          "impact": "Improved Reliability",
          "applied": "Yes",
          "success_rate": 95
        }
      },
      "validation": {
        "validation_score": 100,
        "is_valid": true,
        "errors": [],
        "warnings": []
      },
      "metadata": {
        "adapter_version": "1.0",
        "pipeline_stage": "Capability Adapter",
        "generated_at": "2026-07-27T19:43:10.945234"
      }
    },
    "observations": {
      "state": {
        "current_state": "FAILED",
        "business_state": "Failure",
        "observed": true,
        "observation_type": "STATE",
        "observed_at": "2026-07-27T19:43:11.243102"
      },
      "metrics": {
        "throughput": 607,
        "consumer_lag": 57
      },
      "baseline": {
        "throughput": {
          "current": 607,
          "baseline": 500,
          "deviation_percent": 21.4,
          "status": "WARNING"
        }
      },
      "trend": {
        "throughput": "Increasing"
      },
      "events": [
        "Execution Failure"
      ]
    },
    "metadata": {
      "agent": "Observer Agent",
      "version": "1.0",
      "generated_at": "2026-07-27T19:43:11.243102"
    }
  },
  "behavior_object": {
    "entity_id": "BRO0001",
    "entity_name": "asset.maintenance.alerts",
    "entity_type": "Stream",
    "source_system": "Kafka",
    "event_timestamp": "2026-06-25T00:40:00",
    "observation": {
      "entity_id": "BRO0001",
      "entity_name": "asset.maintenance.alerts",
      "entity_type": "Stream",
      "source_system": "Kafka",
      "event_timestamp": "2026-06-25T00:40:00",
      "entity": {
        "entity_id": "BRO0001",
        "entity_name": "asset.maintenance.alerts",
        "entity_type": "Stream",
        "source_system": "Kafka",
        "execution_status": "FAILED",
        "event_timestamp": "2026-06-25T00:40:00",
        "attributes": {
          "consumer_group": "order-processing-service",
          "partition": 20,
          "offset": 38004142,
          "consumer_lag": 57,
          "throughput": 607,
          "severity": "INFO"
        },
        "context": {
          "business_context": {
            "entity_id": "BRO0001",
            "source_system": "Kafka",
            "business_unit": "Human Resources",
            "owner": "Michael Brown",
            "criticality": "High",
            "sla_minutes": 5,
            "application": "Fraud Detection",
            "cost_center": "HR-201"
          },
          "historical_context": {
            "entity_id": "BRO0001",
            "source_system": "Kafka",
            "avg_runtime_sec": 30,
            "success_rate": 96.39,
            "avg_cpu": 62.0,
            "avg_memory": 57.23,
            "avg_throughput": 500
          },
          "incident_context": {
            "entity_id": "BRO0001",
            "source_system": "Kafka",
            "total_incidents": 9,
            "last_incident": "2026-07-24",
            "root_cause": "Pod Crash",
            "resolution": "Replace Sensor",
            "severity": "Critical"
          },
          "lineage_context": {
            "entity_id": "BRO0001",
            "source_system": "Kafka",
            "depends_on": "DAG0093",
            "downstream": "BRO0001",
            "dependency_type": "Processes"
          },
          "recommendation_context": {
            "entity_id": "BRO0001",
            "source_system": "Kafka",
            "recommendation": "Scale Kubernetes Cluster",
            "impact": "Improved Reliability",
            "applied": "Yes",
            "success_rate": 95
          }
        },
        "validation": {
          "validation_score": 100,
          "is_valid": true,
          "errors": [],
          "warnings": []
        },
        "metadata": {
          "adapter_version": "1.0",
          "pipeline_stage": "Capability Adapter",
          "generated_at": "2026-07-27T19:43:10.945234"
        }
      },
      "observations": {
        "state": {
          "current_state": "FAILED",
          "business_state": "Failure",
          "observed": true,
          "observation_type": "STATE",
          "observed_at": "2026-07-27T19:43:11.243102"
        },
        "metrics": {
          "throughput": 607,
          "consumer_lag": 57
        },
        "baseline": {
          "throughput": {
            "current": 607,
            "baseline": 500,
            "deviation_percent": 21.4,
            "status": "WARNING"
          }
        },
        "trend": {
          "throughput": "Increasing"
        },
        "events": [
          "Execution Failure"
        ]
      },
      "metadata": {
        "agent": "Observer Agent",
        "version": "1.0",
        "generated_at": "2026-07-27T19:43:11.243102"
      }
    },
    "behavior": {
      "deviation": {
        "throughput": {
          "current": 607,
          "baseline": 500,
          "deviation_percent": 21.4,
          "severity": "WARNING"
        }
      },
      "drift": {
        "throughput": {
          "is_drifting": false,
          "occurrences": 1,
          "observations_tracked": 1
        }
      },
      "patterns": [
        "Isolated Metric Anomaly"
      ],
      "behavior_score": 50.0,
      "severity": "WARNING",
      "confidence": 0.62
    },
    "metadata": {
      "agent": "Behavior Agent",
      "version": "1.0",
      "generated_at": "2026-07-27T19:43:11.653986"
    }
  },
  "risk_object": {
    "entity_id": "BRO0001",
    "risk_score": 39.68,
    "risk_severity": "MEDIUM",
    "risk_probability": 30.46,
    "risk_category": "Pipeline Failure",
    "prediction_confidence": 0.6
  },
  "integrity_object": {
    "entity_id": "BRO0001",
    "integrity_score": 97.15,
    "integrity_status": "PASS",
    "output_trust_level": "HIGH",
    "validation_summary": {
      "total_checks": 5,
      "passed": 4,
      "warning": 1,
      "failed": 0,
      "not_evaluated": 0
    },
    "validation_results": {
      "record_count": {
        "status": "WARNING",
        "score": 88.6,
        "details": "actual=607, expected=500, deviation=21.4%"
      },
      "data_quality": {
        "status": "PASS",
        "score": 100.0,
        "details": "validation_score=100.0, errors=0, warnings=0"
      },
      "business_rules": {
        "status": "PASS",
        "score": 100.0,
        "details": "4/4 rules passed"
      },
      "schema": {
        "status": "PASS",
        "score": 100.0,
        "details": "All required fields present"
      },
      "lineage": {
        "status": "PASS",
        "score": 100.0,
        "details": "depends_on=DAG0093, downstream=BRO0001, dependency_type=Processes"
      }
    },
    "validation_details": {
      "record_count": {
        "status": "WARNING",
        "message": "actual=607, expected=500, deviation=21.4%"
      },
      "data_quality": {
        "status": "PASS",
        "message": "validation_score=100.0, errors=0, warnings=0"
      },
      "business_rules": {
        "status": "PASS",
        "message": "4/4 rules passed"
      },
      "schema": {
        "status": "PASS",
        "message": "All required fields present"
      },
      "lineage": {
        "status": "PASS",
        "message": "depends_on=DAG0093, downstream=BRO0001, dependency_type=Processes"
      }
    },
    "primary_failure_reason": "Record Count Validation",
    "execution_time_ms": 0.463,
    "evaluation_timestamp": "2026-07-27T19:43:15.779628",
    "agent_version": "1.0"
  },
  "recommendation_object": {
    "recommendation_id": "8d32ae65-284d-43fe-af14-4af61f5a75b3",
    "entity_id": "BRO0001",
    "priority": "HIGH",
    "recommendation": "Investigate this entity's recent deviation and incident history before the next scheduled run.",
    "reason": "Generated by the deterministic fallback path because an LLM-backed recommendation was not available for this entity.",
    "expected_impact": "Significant — degraded reliability or data freshness likely without action.",
    "estimated_recovery_time": "Short-term (30 minutes - 4 hours)",
    "confidence": 0.3,
    "automation_possible": false,
    "human_approval_required": true,
    "recommendation_source": "deterministic",
    "selected_model": null,
    "generated_timestamp": "2026-07-27T19:43:18.460329",
    "evaluation": {
      "status": "UNAVAILABLE",
      "engine": "ragas",
      "metrics": {},
      "timestamp": "2026-07-27T19:43:18.460329"
    }
  }
}
```

## Status Codes

200 OK

422 Unprocessable Entity

---

# Pipeline Timeline API

## Endpoint

GET /api/v1/dataops/pipelines/{pipelineId}/timeline

## Purpose

Retrieve or construct execution history timestamps for the selected pipeline.

## Request

GET /api/v1/dataops/pipelines/{pipelineId}/timeline

## Path Parameters

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `pipelineId` | string | Yes | The ID of the pipeline (e.g. BRO0001) |

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "pipeline_id": "BRO0001",
  "timeline": [
    {
      "timestamp": "2026-06-01T07:23:00",
      "status": "SUCCESS",
      "metrics": {
        "throughput": 0,
        "consumer_lag": 0,
        "error_rate": 0
      }
    },
    {
      "timestamp": "2026-06-02T15:46:00",
      "status": "SUCCESS",
      "metrics": {
        "throughput": 0,
        "consumer_lag": 0,
        "error_rate": 0
      }
    },
    {
      "timestamp": "2026-06-03T07:18:00",
      "status": "SUCCESS",
      "metrics": {
        "throughput": 0,
        "consumer_lag": 0,
        "error_rate": 0
      }
    },
    {
      "timestamp": "2026-07-26T23:05:00",
      "status": "SUCCESS",
      "metrics": {
        "throughput": 0,
        "consumer_lag": 0,
        "error_rate": 0
      }
    },
    {
      "timestamp": "2026-07-27T07:54:00",
      "status": "SUCCESS",
      "metrics": {
        "throughput": 0,
        "consumer_lag": 0,
        "error_rate": 0
      }
    }
  ]
}
```

## Status Codes

200 OK

422 Unprocessable Entity

---

# Pipeline Dependencies API

## Endpoint

GET /api/v1/dataops/pipelines/{pipelineId}/dependencies

## Purpose

Retrieve lineage, upstream, and downstream dependencies using historical context datasets.

## Request

GET /api/v1/dataops/pipelines/{pipelineId}/dependencies

## Path Parameters

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `pipelineId` | string | Yes | The ID of the pipeline (e.g. BRO0001) |

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "pipeline_id": "BRO0001",
  "upstream_pipelines": [
    "DAG0093"
  ],
  "downstream_pipelines": [
    "BRO0777",
    "MAC0486",
    "PIP0588"
  ],
  "pipeline_lineage": [
    {
      "entity_id": "BRO0001",
      "source_system": "Kafka",
      "depends_on": "DAG0093",
      "downstream": "BRO0001",
      "dependency_type": "Processes"
    },
    {
      "entity_id": "BRO0777",
      "source_system": "Kafka",
      "depends_on": "BRO0001",
      "downstream": "BRO0777",
      "dependency_type": "Processes"
    },
    {
      "entity_id": "PIP0588",
      "source_system": "Azure Data Factory",
      "depends_on": "BRO0001",
      "downstream": "PIP0588",
      "dependency_type": "Triggers Pipeline"
    },
    {
      "entity_id": "MAC0486",
      "source_system": "Manufacturing",
      "depends_on": "BRO0001",
      "downstream": "MAC0486",
      "dependency_type": "Writes To"
    }
  ],
  "dependency_count": 4
}
```

## Status Codes

200 OK

422 Unprocessable Entity

---

# Pipeline Recommendation API

## Endpoint

GET /api/v1/dataops/recommendations/{pipelineId}

## Purpose

Retrieve recommendation assessment metadata for the selected pipeline.

## Request

GET /api/v1/dataops/recommendations/{pipelineId}

## Path Parameters

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `pipelineId` | string | Yes | The ID of the pipeline (e.g. BRO0001) |

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "pipeline_id": "BRO0001",
  "priority": "HIGH",
  "recommendation": "Investigate this entity's recent deviation and incident history before the next scheduled run.",
  "expected_impact": "Significant — degraded reliability or data freshness likely without action.",
  "recovery_time": "Short-term (30 minutes - 4 hours)",
  "automation_possible": false,
  "human_approval_required": true,
  "confidence": 0.3,
  "generated_by": "deterministic",
  "model": null,
  "recommendation_source": "deterministic"
}
```

## Status Codes

200 OK

422 Unprocessable Entity

---

# Operations Copilot Chat API

## Endpoint

POST /api/v1/dataops/copilot/chat

## Purpose

Interface with the Operations Copilot backend to ask questions about pipeline executions.

## Request

POST /api/v1/dataops/copilot/chat

## Path Parameters

None

## Query Parameters

None

## Request Body

```json
{
  "useCaseId": "data-ops",
  "domainId": "operations",
  "question": "Why is BRO0001 critical?",
  "context": {}
}
```

## Example Response

```json
{
  "reply": "Entity BRO0001 failed because of an execution failure and a major metric anomaly. The throughput reached 607 msg/sec (+21.4% above baseline) with consumer lag rising to 57. The Integrity Agent reported record count validation warnings, and a HIGH risk of pipeline failure was detected. Recommended action is to inspect the validation errors, check upstream schemas, and potentially scale consumers.",
  "suggestedActions": [
    "Inspect the validation errors for BRO0001",
    "Check upstream schemas",
    "Scale Kubernetes consumer pods"
  ],
  "confidence": "high",
  "followUpQuestions": [
    "Should we scale the consumer instance?",
    "Do you want to see the detailed lineage graph?"
  ]
}
```

## Status Codes

200 OK

422 Unprocessable Entity

---

# Pipeline Agent Status API

## Endpoint

GET /api/v1/dataops/agents

## Purpose

Retrieve operational health, status, and versions of the AIF Agents.

## Request

GET /api/v1/dataops/agents

## Path Parameters

None

## Query Parameters

None

## Request Body

None

## Example Response

```json
{
  "agents": {
    "Capability Adapter": {
      "status": "UP",
      "version": "1.0.0",
      "health": "OK"
    },
    "Observer": {
      "status": "UP",
      "version": "1.0.0",
      "health": "OK"
    },
    "Behavior": {
      "status": "UP",
      "version": "1.0.0",
      "health": "OK"
    },
    "Risk Prediction": {
      "status": "UP",
      "version": "1.0.0",
      "health": "OK"
    },
    "Integrity": {
      "status": "UP",
      "version": "1.0.0",
      "health": "OK"
    },
    "Recommendation": {
      "status": "UP",
      "version": "1.0.0",
      "health": "OK"
    },
    "Multi LLM": {
      "status": "UP",
      "version": "1.0.0",
      "health": "OK"
    }
  }
}
```

## Status Codes

200 OK

---

## Base URL

http://localhost:8000

Swagger

http://localhost:8000/docs

OpenAPI JSON

http://localhost:8000/openapi.json
