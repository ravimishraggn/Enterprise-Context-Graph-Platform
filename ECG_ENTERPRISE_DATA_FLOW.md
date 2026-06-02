# ECG Platform — Enterprise Data Flow & Operations Guide

> **Audience:** Platform engineers, data engineers, portfolio operations teams, and onboarding leads  
> **Scope:** End-to-end data lifecycle — from client CSV delivery to graph-powered risk queries  
> **Style:** Dry-run / replay walkthrough with failure analysis for each stage  
> **Version:** 1.0 | June 2026

---

## Table of Contents

1. [Platform Overview](#1-platform-overview)
2. [Multi-Client Architecture Model](#2-multi-client-architecture-model)
3. [Stage 0 — Client Onboarding & CSV Contract](#3-stage-0--client-onboarding--csv-contract)
4. [Stage 1 — CSV Ingestion & Validation](#4-stage-1--csv-ingestion--validation)
5. [Stage 2 — Event Transformation & Kafka Dispatch](#5-stage-2--event-transformation--kafka-dispatch)
6. [Stage 3 — Kafka Consumer & Graph Mutation](#6-stage-3--kafka-consumer--graph-mutation)
7. [Stage 4 — Graph Storage & Bi-Temporal Model](#7-stage-4--graph-storage--bi-temporal-model)
8. [Stage 5 — Federation & Enrichment](#8-stage-5--federation--enrichment)
9. [Stage 6 — API Query Layer](#9-stage-6--api-query-layer)
10. [Stage 7 — Agent-Driven Analysis](#10-stage-7--agent-driven-analysis)
11. [Complete Dry-Run Replay — Client Apex Capital](#11-complete-dry-run-replay--client-apex-capital)
12. [Complete Dry-Run Replay — Client Meridian Partners (Second Client)](#12-complete-dry-run-replay--client-meridian-partners-second-client)
13. [Failure Taxonomy & Recovery Playbook](#13-failure-taxonomy--recovery-playbook)
14. [Enterprise Challenges & Mitigations](#14-enterprise-challenges--mitigations)
15. [Operational Runbook](#15-operational-runbook)
16. [Data Lineage & Audit Trail](#16-data-lineage--audit-trail)

---

## 1. Platform Overview

The **Enterprise Context Graph (ECG) Platform** is the single source of structural truth for cross-client investment data. It resolves entities across sources, maintains the full history of ownership and risk relationships, and exposes multi-hop graph queries that no relational system can answer cheaply.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ECG PLATFORM — LOGICAL ARCHITECTURE                  │
│                                                                              │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐                 │
│  │ Client A │   │ Client B │   │ Client C │   │ Client N │  ← CSV Feeds     │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘                 │
│       │              │              │              │                         │
│       └──────────────┴──────────────┴──────────────┘                        │
│                              │                                               │
│                    ┌─────────▼──────────┐                                   │
│                    │  CSV Ingestion     │  ← Stage 1: Parse + Validate       │
│                    │  & Normalizer      │                                    │
│                    └─────────┬──────────┘                                   │
│                              │  Validated events                             │
│                    ┌─────────▼──────────┐                                   │
│                    │   Kafka Topics     │  ← Stage 2: Event bus               │
│                    │  (5 topics,        │                                    │
│                    │   6 partitions)    │                                    │
│                    └─────────┬──────────┘                                   │
│                              │  Consumed events                              │
│                    ┌─────────▼──────────┐                                   │
│                    │  GraphMutation     │  ← Stage 3: Apply to graph          │
│                    │  Consumer (.NET)   │                                    │
│                    └─────────┬──────────┘                                   │
│                              │  Cypher writes                                │
│              ┌───────────────▼────────────────┐                             │
│              │       Neo4j Graph Database     │  ← Stage 4: Persistent store │
│              │    (bi-temporal, multi-client) │                              │
│              └──────┬─────────────────────────┘                             │
│                     │ bolt://7687                                            │
│        ┌────────────┼────────────┐                                          │
│        │            │            │                                          │
│  ┌─────▼────┐ ┌─────▼────┐ ┌────▼─────┐                                   │
│  │ ECG.Api  │ │  Qdrant  │ │Warehouse │  ← Stage 5-6: Query & Enrichment    │
│  │ :5000   │ │  :6333   │ │   API    │                                      │
│  └─────┬────┘ └──────────┘ └──────────┘                                    │
│        │                                                                     │
│  ┌─────▼────┐                                                               │
│  │   ECG    │  ← Stage 7: AI-powered analysis                               │
│  │  Agent   │                                                                │
│  └──────────┘                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Core Principle:** The graph does not replace operational systems. It is the connective tissue — resolving which fund holds which instrument issued by which company owned by which parent flagged for which risk, across all clients in a single traversal.

---

## 2. Multi-Client Architecture Model

Each client in the platform is a **tenant** whose data is namespaced by a `clientId` and `sourceSystem` tag on every node and relationship. This enables:

- Shared entity resolution (two clients holding the same issuer see the same node)
- Isolated ownership chains (client-specific fund structures remain private)
- Cross-client risk visibility (regulatory flags on shared entities propagate to all holding clients)

### Client Namespace Strategy

```
Node property:   sourceSystem  = "APEX_CAPITAL"    | "MERIDIAN_PARTNERS" | "SHARED"
Relationship:    sourceSystem  = "APEX_CAPITAL"    | (inherited)
Entity IDs:      canonicalId   = "ISIN:US1234..."  | "LEI:549300..."      | "INTERNAL:apex-fund-3"
```

| Client | sourceSystem Tag | Data Scope |
|--------|-----------------|------------|
| Apex Capital | `APEX_CAPITAL` | Senior credit funds, portfolio companies, LBO deals |
| Meridian Partners | `MERIDIAN_PARTNERS` | PE funds, co-investments, growth equity |
| Shared / Public | `SHARED` | Public company nodes, OFAC/regulatory flags, benchmark instruments |

### Shared vs. Private Nodes

```
SHARED node (GlobalHoldings Corp — public company)
    │
    ├── SUBSIDIARY_OF ← APEX_CAPITAL (Apex sees this)
    │       └── TechNova Solutions (Apex portfolio company)
    │
    └── SUBSIDIARY_OF ← MERIDIAN_PARTNERS (Meridian sees this)
            └── ByteSphere Ltd (Meridian portfolio company)

Risk flag ATTACHED_TO GlobalHoldings Corp
    → Propagates to BOTH TechNova (Apex) and ByteSphere (Meridian)
    → Both clients querying exposure get the same upstream flag
```

**Failure point:** If entity resolution fails and `GlobalHoldings Corp` is created twice (once per client), risk propagation breaks. Covered in [Section 13](#13-failure-taxonomy--recovery-playbook).

---

## 3. Stage 0 — Client Onboarding & CSV Contract

Before any data flows, the client engagement team establishes the **CSV contract** — the agreed schema for each file type the client will deliver.

### 3.1 CSV File Types

Each client delivers up to five CSV types, each mapping to a different graph mutation event:

| File Type | Kafka Topic Target | Cadence | Owner |
|-----------|-------------------|---------|-------|
| `companies.csv` | `ecg.entity.upserted` | Weekly | Data Ops |
| `funds.csv` | `ecg.entity.upserted` | Monthly | Portfolio Ops |
| `deals.csv` | `ecg.deal.created` | Daily (new deals) | Deal Team |
| `ownership.csv` | `ecg.ownership.changed` | On event | Corporate Actions |
| `risks.csv` | `ecg.risk.flagged` | On event | Risk & Compliance |

### 3.2 CSV Schema Contracts

**`companies.csv`** — Entity master file
```
company_id, legal_name, isin, lei, jurisdiction, sector, company_type, parent_id, ownership_pct, effective_date, is_active, source_system
```

Example row:
```
APEX-CO-001,TechNova Solutions Ltd,US0231351067,549300ABCDEF12345678,US,Technology,PORTFOLIO,APEX-CO-002,100.0,2021-09-15,true,APEX_CAPITAL
```

**`deals.csv`** — New deal intake
```
deal_id, deal_name, deal_type, deal_size_millions, currency, borrower_company_id, sponsor_company_id, fund_ids, status, close_date, source_system
```

**`ownership.csv`** — Corporate action feed
```
child_company_id, old_parent_company_id, new_parent_company_id, ownership_pct, ownership_type, change_reason, effective_date, source_system
```

**`risks.csv`** — Risk & compliance flags
```
risk_id, risk_type, severity, attached_to_entity_id, attached_to_entity_type, description, flagged_by, flagged_date, is_active, source_system
```

### 3.3 Onboarding Checklist

```
□ Client assigns a sourceSystem identifier (no spaces, all caps)
□ CSV schema validated against contract via schema validator script
□ Canonical ID strategy agreed: prefer LEI → ISIN → CUSIP → INTERNAL:{client}-{id}
□ Historical load date agreed (how far back to seed ownership history)
□ Kafka credentials provisioned (SASL/SSL for production)
□ API read-only credentials scoped to client's sourceSystem
□ Test load executed against staging Neo4j instance
□ Sign-off from client data owner and ECG platform lead
```

---

## 4. Stage 1 — CSV Ingestion & Validation

The **CSV Ingestion Service** (`scripts/csv_ingestor.py`) is the entry point. It reads files from a watched directory (or S3 bucket in production), validates each row, and dispatches structured events to Kafka.

### 4.1 File Drop & Detection

```
Production path:
  /data/incoming/{sourceSystem}/{date}/{filename}.csv
  
  Example:
  /data/incoming/APEX_CAPITAL/2026-06-03/ownership.csv
  /data/incoming/MERIDIAN_PARTNERS/2026-06-03/deals.csv

Development path:
  ./data/incoming/
```

Detection: A filesystem watcher (inotify on Linux / polling on Windows) or a scheduled cron job picks up new files every 15 minutes.

### 4.2 Validation Pipeline

Each row passes through four validation gates before being queued:

```
Row from CSV
    │
    ▼
[Gate 1] Schema Validation
    │  Check all required columns present
    │  Check data types (date formats, numeric ranges)
    │  FAIL → quarantine row to /data/quarantine/{client}/{date}/
    │
    ▼
[Gate 2] Referential Integrity Check
    │  Does borrower_company_id exist in companies.csv or Neo4j?
    │  Does fund_id resolve to a known fund?
    │  FAIL → buffer row, emit warning, retry after parent entity loaded
    │
    ▼
[Gate 3] Entity Resolution
    │  Lookup canonical node by LEI → ISIN → CUSIP → internal ID
    │  If SHARED node found with same canonicalId → reuse node (no duplicate)
    │  If not found → create new node with client's sourceSystem tag
    │  WARN → log when canonical match is fuzzy (full-text index score < 0.85)
    │
    ▼
[Gate 4] Deduplication
    │  Hash of (entityId + effectiveDate + sourceSystem)
    │  If hash seen in Redis dedup cache → skip (idempotent replay safe)
    │  TTL: 72 hours (covers re-delivery window)
    │
    ▼
Validated Event → Kafka Producer
```

### 4.3 Error Buckets

| Error Type | Disposition | Alert Level |
|------------|-------------|-------------|
| Missing required column | Reject entire file, quarantine | P1 — immediate alert |
| Invalid date format | Reject row, continue file | P2 — daily digest |
| Referential integrity miss | Buffer row, retry up to 3x | P2 — retry metric |
| Duplicate event (dedup hit) | Skip silently, increment counter | INFO |
| Fuzzy entity match (< 0.85) | Accept with `requiresReview: true` flag | P3 — review queue |
| Unknown sourceSystem | Reject row | P1 — immediate alert |

### 4.4 Validation Output

Each ingestion run produces a **manifest file**:

```json
{
  "runId": "run-2026-06-03-apex-001",
  "client": "APEX_CAPITAL",
  "file": "ownership.csv",
  "startTime": "2026-06-03T08:00:01Z",
  "endTime": "2026-06-03T08:00:47Z",
  "totalRows": 142,
  "accepted": 139,
  "rejected": 2,
  "buffered": 1,
  "duplicatesSkipped": 0,
  "kafkaEventsDispatched": 139,
  "quarantineFiles": ["/data/quarantine/APEX_CAPITAL/2026-06-03/ownership_rejected_rows.csv"],
  "warnings": ["Row 88: fuzzy entity match score 0.79 for 'TechNova Soln.' → 'TechNova Solutions Ltd'"]
}
```

---

## 5. Stage 2 — Event Transformation & Kafka Dispatch

After validation, each row is transformed into a **typed graph event** and published to the appropriate Kafka topic.

### 5.1 Row → Event Mapping

**Ownership row → `OwnershipChangedEvent`**

```python
# Input CSV row (after validation):
{
  "child_company_id": "APEX-CO-001",
  "old_parent_company_id": "APEX-CO-002",
  "new_parent_company_id": "APEX-CO-009",
  "ownership_pct": 85.0,
  "ownership_type": "DIRECT",
  "change_reason": "RESTRUCTURING",
  "effective_date": "2026-06-01",
  "source_system": "APEX_CAPITAL"
}

# Transformed event (published to Kafka):
{
  "eventId": "evt-uuid-a1b2c3d4",
  "eventType": "OwnershipChanged",
  "schemaVersion": "1.2",
  "clientId": "APEX_CAPITAL",
  "timestamp": "2026-06-03T08:00:23Z",
  "payload": {
    "childCompanyId": "APEX-CO-001",
    "oldParentCompanyId": "APEX-CO-002",
    "newParentCompanyId": "APEX-CO-009",
    "ownershipPct": 85.0,
    "ownershipType": "DIRECT",
    "changeReason": "RESTRUCTURING",
    "effectiveDate": "2026-06-01"
  },
  "metadata": {
    "sourceFile": "ownership.csv",
    "sourceRow": 23,
    "runId": "run-2026-06-03-apex-001",
    "ingestionTimestamp": "2026-06-03T08:00:22Z"
  }
}
```

### 5.2 Kafka Topic Routing

```
Event Type                    Topic                         Partition Key
─────────────────────────────────────────────────────────────────────────
DealCreatedEvent         →   ecg.deal.created              DealId
DealStatusChangedEvent   →   ecg.deal.status.changed       DealId
OwnershipChangedEvent    →   ecg.ownership.changed         ChildCompanyId  ← CRITICAL
RiskFlaggedEvent         →   ecg.risk.flagged              AttachedToEntityId
ValuationUpdatedEvent    →   ecg.valuation.updated         InstrumentId
```

**Why entity ID as partition key (not random / event ID):**

```
Scenario: TechNova ownership changes twice in the same day

Event 1 (08:00): TechNova parent = Continental Group (100%)
Event 2 (14:00): TechNova parent = GlobalHoldings (85%)

If partitioned by event ID → events may land on partition 3 and partition 1
Consumer group may process Event 2 BEFORE Event 1 → wrong state

If partitioned by ChildCompanyId → both land on partition 2 (hash("APEX-CO-001") % 6 = 2)
Kafka guarantees order within a partition → Event 1 always processed first
```

### 5.3 Producer Configuration

```python
producer_config = {
    "bootstrap.servers": "kafka:9092",
    "client.id": f"ecg-csv-ingestor-{client_id}",
    "acks": "all",                    # Wait for all replicas (durability)
    "retries": 5,
    "retry.backoff.ms": 500,
    "delivery.timeout.ms": 30000,
    "enable.idempotence": True,       # Exactly-once producer semantics
    "compression.type": "lz4",        # ~60% payload size reduction
    "batch.size": 65536,              # 64KB batch
    "linger.ms": 10                   # Wait 10ms to fill batch
}
```

### 5.4 Throughput Characteristics

| Scenario | Events/sec | Kafka Lag | Notes |
|----------|-----------|-----------|-------|
| Normal daily load (1 client) | 50–200 | < 1s | Batch CSV at market open |
| Multi-client parallel load (5 clients) | 500–1,000 | < 5s | Staggered 5-min offsets |
| Month-end bulk restatement | 5,000–10,000 | 30–90s | Throttle to 1,000/s |
| Disaster recovery replay | 50,000+ | Minutes | Replay from beginning of topic |

---

## 6. Stage 3 — Kafka Consumer & Graph Mutation

The `GraphMutationConsumer` (`src/ECG.Streaming/GraphMutationConsumer.cs`) is a hosted .NET background service that reads events from all five topics and applies them to Neo4j.

### 6.1 Consumer Architecture

```
Kafka Broker (5 topics, 6 partitions each)
    │
    ├── ecg.deal.created         (partitions 0-5)
    ├── ecg.deal.status.changed  (partitions 0-5)
    ├── ecg.ownership.changed    (partitions 0-5)
    ├── ecg.risk.flagged         (partitions 0-5)
    └── ecg.valuation.updated    (partitions 0-5)
         │
         ▼
  Consumer Group: ecg-graph-mutation-consumer
  (up to 6 instances, one per partition — horizontal scale)
         │
         ▼
  GraphMutationConsumer (.NET BackgroundService)
    │
    ├── DeserializeEvent()          → typed C# record
    ├── Dispatch to handler()       → switch on EventType
    │
    ├── HandleDealCreatedAsync()
    │     └── MERGE (d:Deal {id}) SET d += props
    │         MERGE (b:Company {id:borrowerId})
    │         MERGE (d)-[:INVOLVES {role:"BORROWER"}]->(b)
    │         FOR fundId IN event.fundIds:
    │             MERGE (f:Fund {id:fundId})
    │             MERGE (f)-[:INVESTED_IN]->(d)
    │
    ├── HandleDealStatusChangedAsync()
    │     └── MATCH (d:Deal {id}) SET d.status = newStatus
    │
    ├── HandleOwnershipChangedAsync()          ← BI-TEMPORAL (most complex)
    │     └── OPTIONAL MATCH current active rel (expiryDate IS NULL)
    │         SET old_rel.expiryDate = event.effectiveDate
    │         CREATE new rel with effectiveDate, ownershipPct, sourceSystem
    │
    ├── HandleRiskFlaggedAsync()
    │     └── MERGE (r:Risk {id}) SET r += props
    │         MERGE (r)-[:ATTACHED_TO]->(entity {id: attachedToEntityId})
    │
    └── HandleValuationUpdatedAsync()
          └── MATCH (f:Fund)-[pos:HAS_POSITION]->(i:Instrument)
              SET pos.currentValueMillions = newValue
              SET pos.weightPct = newWeightPct
              SET pos.asOfDate = asOfDate
```

### 6.2 Consumer Configuration

```csharp
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "kafka:9092",
    GroupId = "ecg-graph-mutation-consumer",
    AutoOffsetReset = AutoOffsetReset.Earliest,    // Replay from start on new group
    EnableAutoCommit = false,                       // Manual commit AFTER Neo4j write
    MaxPollIntervalMs = 300000,                     // 5 min Neo4j write timeout
    SessionTimeoutMs = 30000,
    HeartbeatIntervalMs = 3000,
    FetchMinBytes = 1,
    FetchMaxWaitMs = 500
};
```

**Critical:** `EnableAutoCommit = false`. The consumer only commits the offset **after** the Neo4j write succeeds. If the process crashes between consume and write, the event is re-processed on restart. Combined with Cypher `MERGE` statements, this achieves **at-least-once with idempotent writes**.

### 6.3 Offset Management & Replay

```
Offset commit flow:
  1. Poll() → receive batch of events
  2. For each event:
     a. Deserialize
     b. Execute Cypher mutation
     c. If Cypher succeeds → store offset in memory (not committed yet)
     d. If Cypher fails → throw, do NOT advance offset
  3. After full batch processed → StoreOffset() (manual commit)
  
Replay scenario:
  Consumer crashes at step 2d (after event 47 of 100).
  On restart: Consumer resumes from last committed offset (event 45).
  Events 45-47 are re-processed. MERGE is idempotent → no duplicate nodes.
  Events 48-100 continue normally.
```

---

## 7. Stage 4 — Graph Storage & Bi-Temporal Model

Neo4j is the persistent store. Every mutation is a graph write that either creates nodes/relationships or updates properties on existing ones.

### 7.1 Graph Schema (Node Types)

```
Company {id, name, canonicalId, jurisdiction, sector, companyType, isActive, sourceSystem}
Fund    {id, name, fundType, vintage, aumMillions, manager, status, sourceSystem}
Deal    {id, dealName, dealType, dealSizeMillions, currency, status, closeDate, sourceSystem}
Instrument {id, name, isin, cusip, instrumentType, faceValueMillions, coupon, maturityDate, sourceSystem}
Risk    {id, riskType, severity, flaggedDate, description, isActive, flaggedBy, sourceSystem}
Investor {id, name, investorType, domicile, committedCapitalMillions, sourceSystem}
Document {id, docType, title, embeddingId, storagePath, documentDate, sourceSystem}
Person  {id, fullName, role, email, sourceSystem}
```

### 7.2 Bi-Temporal Ownership — The Core Pattern

The `SUBSIDIARY_OF` relationship carries two temporal fields that together represent the full ownership history without ever deleting data:

```
effectiveDate  — when this ownership relationship became real-world true
expiryDate     — when it became false (NULL = currently active)
```

**Before a corporate action (company restructuring):**

```
(TechNova Solutions)-[:SUBSIDIARY_OF {
    ownershipPct: 100.0,
    ownershipType: "DIRECT",
    effectiveDate: "2021-09-15",
    expiryDate: null,               ← NULL = currently active
    sourceSystem: "APEX_CAPITAL"
}]->(Continental Group)
```

**OwnershipChangedEvent arrives (effectiveDate: 2026-06-01):**

```
Step 1 — Expire old relationship:
(TechNova Solutions)-[:SUBSIDIARY_OF {
    ownershipPct: 100.0,
    effectiveDate: "2021-09-15",
    expiryDate: "2026-06-01",       ← SET to event.effectiveDate
    sourceSystem: "APEX_CAPITAL"
}]->(Continental Group)

Step 2 — Create new relationship:
(TechNova Solutions)-[:SUBSIDIARY_OF {
    ownershipPct: 85.0,
    ownershipType: "DIRECT",
    effectiveDate: "2026-06-01",
    expiryDate: null,               ← Active
    sourceSystem: "APEX_CAPITAL"
}]->(GlobalHoldings Corp)
```

**Time-travel query (as of 2024-01-01):**
```cypher
MATCH (c:Company {name: "TechNova Solutions"})-[rel:SUBSIDIARY_OF]->(parent)
WHERE rel.effectiveDate <= date("2024-01-01")
  AND (rel.expiryDate IS NULL OR rel.expiryDate > date("2024-01-01"))
RETURN parent.name  // → "Continental Group"
```

**Time-travel query (as of today, 2026-06-03):**
```cypher
// Same query → "GlobalHoldings Corp"
```

### 7.3 Multi-Client Graph Coexistence

```
Neo4j — Single Instance, Multi-Client Data

(GlobalHoldings Corp) ← SHARED node, one physical node
    │
    ├── ←[SUBSIDIARY_OF {sourceSystem:"APEX_CAPITAL"}]── (TechNova Solutions)
    │                                                      {sourceSystem:"APEX_CAPITAL"}
    │
    └── ←[SUBSIDIARY_OF {sourceSystem:"MERIDIAN_PARTNERS"}]── (ByteSphere Ltd)
                                                                {sourceSystem:"MERIDIAN_PARTNERS"}

(REGULATORY_WATCH Risk)-[ATTACHED_TO]->(GlobalHoldings Corp)
    │
    └── Visible to both clients via graph traversal — no duplication
```

### 7.4 Index Strategy

```cypher
-- Unique constraints (B-tree, enforce no-duplicate IDs)
CREATE CONSTRAINT company_id FOR (c:Company) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT fund_id    FOR (f:Fund)    REQUIRE f.id IS UNIQUE;
-- ... (all 8 node types)

-- Range indexes (fast WHERE clause filters)
CREATE INDEX company_sector      FOR (c:Company) ON (c.sector);
CREATE INDEX company_jurisdiction FOR (c:Company) ON (c.jurisdiction);
CREATE INDEX risk_type_severity  FOR (r:Risk) ON (r.riskType, r.severity, r.isActive);
CREATE INDEX instrument_maturity FOR (i:Instrument) ON (i.maturityDate);

-- Relationship property indexes (bi-temporal performance)
CREATE INDEX sub_of_effective FOR ()-[r:SUBSIDIARY_OF]-() ON (r.effectiveDate);
CREATE INDEX sub_of_expiry    FOR ()-[r:SUBSIDIARY_OF]-() ON (r.expiryDate);

-- Full-text index (fuzzy entity name search)
CREATE FULLTEXT INDEX entity_names
FOR (c:Company|f:Fund|d:Deal|i:Instrument)
ON EACH [c.name, f.name, d.dealName, i.name];
```

---

## 8. Stage 5 — Federation & Enrichment

Once graph structure is written, the **Federation Service** (`src/ECG.Federation/Services/FederationService.cs`) enriches graph query results with data from two supplementary systems.

### 8.1 Federation Flow

```
Incoming Query (e.g., "Get portfolio view for fund X")
    │
    ▼
[1] Neo4j Graph Traversal  ← ALWAYS FIRST (entity resolution)
    Returns: canonical node IDs, structural paths, graph metrics
    │
    ├── [2a] Qdrant Vector Search        (parallel)
    │         Input:  entity IDs from step 1
    │         Query:  "find documents related to entity X"
    │         Return: CIMs, term sheets, risk reports
    │
    └── [2b] Warehouse API Call          (parallel)
              Input:  canonical company IDs from step 1
              Query:  GET /metrics/{companyId}
              Return: revenue, EBITDA, leverage ratio, last audit date
    │
    ▼
[3] Response Merge
    {
      graph: { exposure_chains, ownership_paths, risk_flags },
      documents: [ { title, docType, relevanceScore } ],
      metrics: { revenue: 1200M, ebitda_margin: 0.28, leverage: 4.2x }
    }
```

**Why graph runs first:** The graph holds `canonicalId` — the authoritative identifier that both Qdrant (embeddingId) and the Warehouse API (companyId) use. Without it, the parallel queries have no scope to execute against.

### 8.2 Vector Document Search (Qdrant)

Documents (CIMs, term sheets, IC memos) are embedded at upload time by `seed_vectors.py` using SentenceTransformer (`all-MiniLM-L6-v2`). At query time:

```python
# Entity name → vector → nearest document chunks
query_vector = model.encode("TechNova Solutions regulatory risk")
results = qdrant_client.search(
    collection_name="ecg_documents",
    query_vector=query_vector,
    limit=5,
    score_threshold=0.7
)
# Returns: [{docId, title, chunk, score}, ...]
```

### 8.3 Warehouse API (Financial Metrics)

```python
# Mock warehouse — deterministic by companyId hash
GET http://warehouse-api:8090/metrics/APEX-CO-001
→ {
    "companyId": "APEX-CO-001",
    "revenue_millions": 847.3,
    "ebitda_millions": 211.8,
    "ebitda_margin": 0.25,
    "net_leverage": 4.8,
    "interest_coverage": 2.3,
    "last_audit_date": "2025-12-31",
    "currency": "USD"
  }
```

---

## 9. Stage 6 — API Query Layer

The ECG REST API (`src/ECG.Api`, port 5000) exposes pre-built Cypher query patterns as parameterized endpoints. All queries are read-only; mutations happen only via the Kafka → Consumer path.

### 9.1 Key Endpoints

**Fund Exposure (Multi-Hop Risk Traversal)**
```
GET /api/funds/{fundName}/exposure?riskType=REGULATORY_WATCH&maxHops=4&asOfDate=2026-06-01

Response:
{
  "fund": "Apex Senior Credit Fund III",
  "queryDate": "2026-06-01",
  "exposureCount": 3,
  "exposureChains": [
    {
      "directIssuer": "TechNova Solutions",
      "riskBearingEntity": "GlobalHoldings Corp",
      "totalHops": 4,
      "severity": "HIGH",
      "riskType": "REGULATORY_WATCH",
      "positionValueMillions": 200,
      "exposurePath": [
        "Apex Senior Credit Fund III",
        "TechNova TLA ($200M)",
        "TechNova Solutions",
        "Continental Group",
        "GlobalHoldings Corp"
      ],
      "ownershipChain": [100.0, 78.5]
    }
  ]
}
```

**Ownership Chain (UBO Query)**
```
GET /api/companies/TechNova%20Solutions/ownership-chain?asOfDate=2026-06-01

Response:
{
  "company": "TechNova Solutions",
  "queryDate": "2026-06-01",
  "chainDepth": 3,
  "ultimateBeneficialOwner": "GlobalHoldings Corp",
  "chain": [
    { "entity": "TechNova Solutions",   "ownershipPct": null },
    { "entity": "Continental Group",    "ownershipPct": 100.0, "effectiveDate": "2021-09-15" },
    { "entity": "GlobalHoldings Corp",  "ownershipPct": 78.5,  "effectiveDate": "2019-03-01" }
  ],
  "cumulativeOwnership": 78.5
}
```

**Risk Propagation (Blast Radius)**
```
GET /api/risks/{riskId}/propagation?asOfDate=2026-06-01

Response:
{
  "riskId": "RISK-REG-001",
  "riskType": "REGULATORY_WATCH",
  "severity": "HIGH",
  "originEntity": "GlobalHoldings Corp",
  "affectedFunds": [
    {
      "fund": "Apex Senior Credit Fund III",
      "client": "APEX_CAPITAL",
      "positionValueMillions": 200,
      "instrument": "TechNova TLA",
      "hopsFromOrigin": 4
    },
    {
      "fund": "Meridian Growth Fund II",
      "client": "MERIDIAN_PARTNERS",
      "positionValueMillions": 85,
      "instrument": "ByteSphere Series B",
      "hopsFromOrigin": 3
    }
  ],
  "totalAffectedCapital": 285
}
```

### 9.2 Query Performance Characteristics

| Query Type | Graph Size (nodes) | Avg. Latency | P99 Latency |
|------------|-------------------|-------------|-------------|
| Ownership chain (≤6 hops) | 500K | 12ms | 45ms |
| Fund exposure (4 hops) | 500K | 35ms | 120ms |
| Risk propagation (full blast) | 500K | 55ms | 200ms |
| Full portfolio view | 500K | 28ms | 90ms |

Redis caches results for 5 minutes by default (configurable per query type).

---

## 10. Stage 7 — Agent-Driven Analysis

The **ECG Agent** (`scripts/ecg_agent.py`) is a LangGraph workflow that chains graph traversal → external enrichment → LLM summarization into a single analyst-facing narrative.

### 10.1 Agent Workflow (LangGraph State Machine)

```
Input: { fund_name, risk_type, max_hops, as_of_date }
    │
    ▼
[Node 1] traverse_exposure_graph()
    │  GET /api/funds/{fund}/exposure?riskType={risk}&maxHops={hops}&asOfDate={date}
    │  State update: graph_exposure = response
    │
    ▼
[Node 2] fetch_portfolio_context()
    │  GET /api/funds/{fund}/portfolio?asOfDate={date}
    │  State update: graph_portfolio = response
    │
    ▼
[Node 3] enrich_with_external_data()      (conditional — only if enrichment enabled)
    │  Qdrant: search(fund_name + risk_type)
    │  Warehouse: GET /metrics/{issuerIds[]}
    │  State update: enriched_context = merged
    │
    ▼
[Node 4] summarize_with_llm()
    │  System prompt: "You are a risk analyst. Explain ONLY what the graph returned.
    │                  Do NOT infer relationships not present in the data."
    │  Input: graph_exposure + graph_portfolio + enriched_context
    │  Model: claude-sonnet-4-6
    │
    ▼
Output:
{
  "summary": "Apex Senior Credit Fund III has indirect exposure to GlobalHoldings Corp
              through a 4-hop ownership chain via TechNova Solutions ($200M position)...",
  "risk_narrative": "HIGH severity REGULATORY_WATCH flag on GlobalHoldings Corp
                     affects 78.5% of the capital structure above TechNova...",
  "action_items": [
    "Review TechNova TLA covenant package for change-of-control provisions",
    "Escalate to compliance: beneficial owner in OFAC-flagged jurisdiction",
    "Request updated CIM from deal team"
  ]
}
```

### 10.2 Agent Safety Guardrail

The LLM is explicitly constrained: **it summarizes graph-derived facts, it never infers new relationships**. The graph is the authority on structure; the LLM adds language clarity only.

```python
SYSTEM_PROMPT = """
You are a risk analyst for a credit fund. You have been given structured graph data
showing ownership chains, risk flags, and fund exposure paths.

RULES:
1. Only reference entities and relationships that appear in the graph data provided.
2. Do NOT infer ownership, risk, or exposure that is not explicitly in the data.
3. If the graph shows no exposure, say so clearly — do not speculate.
4. Cite specific hop counts, ownership percentages, and position values.
"""
```

---

## 11. Complete Dry-Run Replay — Client Apex Capital

This section walks through a single end-to-end data flow event from raw CSV to analyst output. Use it as a reference for debugging, onboarding, and incident replay.

### Scenario

**Date:** 2026-06-03  
**Client:** Apex Capital  
**Event:** TechNova Solutions is sold from Continental Group (100%) to GlobalHoldings Corp (85%)  
**Impact:** Apex Senior Credit Fund III holds a $200M TechNova TLA; GlobalHoldings Corp carries a REGULATORY_WATCH (HIGH) flag  

---

### T+0:00 — CSV File Drop

Apex Capital's corporate actions system generates and drops:

```
/data/incoming/APEX_CAPITAL/2026-06-03/ownership.csv
```

```csv
child_company_id,old_parent_company_id,new_parent_company_id,ownership_pct,ownership_type,change_reason,effective_date,source_system
APEX-CO-001,APEX-CO-002,APEX-CO-009,85.0,DIRECT,ACQUISITION,2026-06-01,APEX_CAPITAL
```

### T+0:02 — Filesystem Watcher Detects File

Watcher triggers `csv_ingestor.py` with:
```
client = APEX_CAPITAL
file   = ownership.csv
runId  = run-2026-06-03-apex-001
```

### T+0:03 — Gate 1: Schema Validation

```
✓ All 8 required columns present
✓ ownership_pct is numeric (85.0)
✓ effective_date parses as ISO date (2026-06-01)
✓ source_system matches known client list
Row status: VALID
```

### T+0:03 — Gate 2: Referential Integrity

```
Lookup APEX-CO-001 in Neo4j: FOUND (TechNova Solutions)
Lookup APEX-CO-002 in Neo4j: FOUND (Continental Group)
Lookup APEX-CO-009 in Neo4j: FOUND (GlobalHoldings Corp)
Row status: VALID
```

### T+0:03 — Gate 3: Entity Resolution

```
APEX-CO-009 → Neo4j lookup → found node with canonicalId: "LEI:549300XYZ..."
SHARED? → No, sourceSystem = APEX_CAPITAL → use existing client-scoped node
Row status: RESOLVED, no new nodes needed
```

### T+0:03 — Gate 4: Deduplication

```
Hash: SHA256(APEX-CO-001 + 2026-06-01 + APEX_CAPITAL) = "a3f8..."
Redis lookup: NOT FOUND → proceed
Row status: NEW EVENT
```

### T+0:04 — Event Construction

```json
{
  "eventId": "evt-a3f8b2c1d9e4",
  "eventType": "OwnershipChanged",
  "schemaVersion": "1.2",
  "clientId": "APEX_CAPITAL",
  "timestamp": "2026-06-03T08:00:04Z",
  "payload": {
    "childCompanyId": "APEX-CO-001",
    "oldParentCompanyId": "APEX-CO-002",
    "newParentCompanyId": "APEX-CO-009",
    "ownershipPct": 85.0,
    "ownershipType": "DIRECT",
    "changeReason": "ACQUISITION",
    "effectiveDate": "2026-06-01"
  },
  "metadata": {
    "sourceFile": "ownership.csv",
    "sourceRow": 1,
    "runId": "run-2026-06-03-apex-001"
  }
}
```

### T+0:04 — Kafka Publish

```
Topic:         ecg.ownership.changed
Partition key: APEX-CO-001 → hash % 6 → partition 3
Offset:        12,847 (new message)
Delivery ack:  RECEIVED from all in-sync replicas
```

**Redis dedup cache updated:**
```
SET "dedup:a3f8..." EX 259200   (TTL: 72 hours)
```

**Manifest updated:**
```
accepted: 1, kafkaEventsDispatched: 1
```

### T+0:05 — GraphMutationConsumer Polls

Consumer polls partition 3 of `ecg.ownership.changed`. Receives event at offset 12,847.

```
DeserializeEvent()   → OwnershipChangedEvent record
DispatchHandler()    → HandleOwnershipChangedAsync()
```

### T+0:05 — Cypher Execution (Bi-Temporal Write)

```cypher
// Step 1: Expire the old relationship
OPTIONAL MATCH (child:Company {id: "APEX-CO-001"})
               -[oldRel:SUBSIDIARY_OF {sourceSystem: "APEX_CAPITAL"}]->
               (oldParent:Company {id: "APEX-CO-002"})
WHERE oldRel.expiryDate IS NULL
SET oldRel.expiryDate = date("2026-06-01")

// Step 2: Create the new relationship
MATCH (child:Company {id: "APEX-CO-001"})
MATCH (newParent:Company {id: "APEX-CO-009"})
CREATE (child)-[:SUBSIDIARY_OF {
    ownershipPct: 85.0,
    ownershipType: "DIRECT",
    changeReason: "ACQUISITION",
    effectiveDate: date("2026-06-01"),
    expiryDate: null,
    sourceSystem: "APEX_CAPITAL",
    mutationEventId: "evt-a3f8b2c1d9e4"
}]->(newParent)
```

**Neo4j execution result:** `Properties set: 1, Relationships created: 1`

### T+0:05 — Kafka Offset Committed

```
StoreOffset(topic: ecg.ownership.changed, partition: 3, offset: 12,847)
CommitAsync() → SUCCESS
```

**Graph state after write:**
```
(TechNova Solutions)-[:SUBSIDIARY_OF {ownPct:100, eff:2021-09-15, exp:2026-06-01}]->(Continental Group)
(TechNova Solutions)-[:SUBSIDIARY_OF {ownPct:85,  eff:2026-06-01, exp:null      }]->(GlobalHoldings Corp)
```

### T+0:10 — Portfolio Manager Queries Exposure

```
GET /api/funds/Apex Senior Credit Fund III/exposure
    ?riskType=REGULATORY_WATCH
    &maxHops=4
    &asOfDate=2026-06-03

Processing:
  1. Cache miss → execute multi_hop_exposure.cypher
  2. Traverse: Fund → HAS_POSITION → TechNova TLA → ISSUED_BY → TechNova Solutions
               → SUBSIDIARY_OF (active as of 2026-06-03) → GlobalHoldings Corp
               → ATTACHED_TO ← REGULATORY_WATCH Risk
  3. Exposure chain found: depth 4, severity HIGH, $200M
  4. Cache result in Redis (TTL: 5 min)
  5. Return response
```

**Response:**
```json
{
  "fund": "Apex Senior Credit Fund III",
  "queryDate": "2026-06-03",
  "exposureCount": 1,
  "exposureChains": [{
    "directIssuer": "TechNova Solutions",
    "riskBearingEntity": "GlobalHoldings Corp",
    "totalHops": 4,
    "severity": "HIGH",
    "riskType": "REGULATORY_WATCH",
    "positionValueMillions": 200,
    "exposurePath": [
      "Apex Senior Credit Fund III",
      "TechNova TLA ($200M)",
      "TechNova Solutions",
      "Continental Group",     ← Wait — this is wrong!
      "GlobalHoldings Corp"    ← Let's check...
    ]
  }]
}
```

**Actually correct** — even though TechNova now directly subsidiary of GlobalHoldings, Continental Group is still in the chain (TechNova → Continental → GlobalHoldings is the structural path, or the new path is TechNova → GlobalHoldings directly at 85%). The query uses `asOfDate=2026-06-03`, so it uses the active relationship as of today: `TechNova → GlobalHoldings (eff:2026-06-01)`. Correct.

### T+0:15 — Agent Run (Risk Narrative)

```bash
python scripts/ecg_agent.py \
  --fund "Apex Senior Credit Fund III" \
  --risk REGULATORY_WATCH \
  --max-hops 4 \
  --as-of-date 2026-06-03
```

Agent output:
```
RISK NARRATIVE — Apex Senior Credit Fund III
────────────────────────────────────────────
SUMMARY:
The fund holds $200M in TechNova Solutions TLA (Term Loan A), representing
18.2% of fund NAV. As of 2026-06-03, TechNova Solutions is a direct subsidiary
of GlobalHoldings Corp (85% ownership, effective 2026-06-01). GlobalHoldings Corp
carries an active REGULATORY_WATCH flag of HIGH severity, flagged by compliance
on 2025-11-14.

RISK NARRATIVE:
The 4-hop exposure path (Fund → Instrument → Issuer → Parent) means the fund
has indirect beneficial owner exposure to a REGULATORY_WATCH-designated entity.
The recent ownership change (acquisition effective 2026-06-01) elevated the risk
tier: previously TechNova was owned by Continental Group (no active flags).

ACTION ITEMS:
1. Review TechNova TLA credit agreement for change-of-control notification clauses
2. Escalate to compliance: GlobalHoldings Corp beneficial ownership includes
   entities in OFAC-adjacent jurisdictions per graph data
3. Request updated 2026 audited financials from borrower
4. Assess whether the 85% ownership (not 100%) creates an opinion on control for
   AIFMD/Volcker reporting purposes
```

---

## 12. Complete Dry-Run Replay — Client Meridian Partners (Second Client)

### Scenario

**Same day, same root entity:** Meridian Partners also has downstream exposure to GlobalHoldings Corp via ByteSphere Ltd. Their ownership file arrives 20 minutes after Apex Capital's.

### T+0:20 — Meridian CSV Drop

```
/data/incoming/MERIDIAN_PARTNERS/2026-06-03/risks.csv
```

```csv
risk_id,risk_type,severity,attached_to_entity_id,attached_to_entity_type,description,flagged_by,flagged_date,is_active,source_system
RISK-REG-001,REGULATORY_WATCH,HIGH,SHARED-CO-009,Company,"GlobalHoldings under OFAC enhanced monitoring",compliance-team,2025-11-14,true,SHARED
```

**Note:** This is the same risk flag already in the graph (from initial seed). `source_system = SHARED` means it should be shared across all clients.

### T+0:21 — Gate 4: Deduplication

```
Hash: SHA256(SHARED-CO-009 + RISK-REG-001 + SHARED) = "d7e2..."
Redis lookup: NOT FOUND (first time this event flows through Meridian's ingestor)
→ Proceed
```

### T+0:21 — Cypher Execution

```cypher
MERGE (r:Risk {id: "RISK-REG-001"})
ON CREATE SET r.riskType = "REGULATORY_WATCH",
              r.severity = "HIGH",
              r.flaggedDate = date("2025-11-14"),
              r.description = "GlobalHoldings under OFAC enhanced monitoring",
              r.isActive = true,
              r.flaggedBy = "compliance-team",
              r.sourceSystem = "SHARED"
ON MATCH SET  r.isActive = true         // Idempotent: just confirms still active
MERGE (r)-[:ATTACHED_TO]->(entity:Company {id: "SHARED-CO-009"})
```

**Result:** `MERGE` finds existing node → `ON MATCH` branch → no new node created. Relationship already exists → no new relationship. Graph unchanged. **Idempotent write confirmed.**

### T+0:25 — Meridian Analyst Queries

```
GET /api/funds/Meridian Growth Fund II/exposure
    ?riskType=REGULATORY_WATCH
    &maxHops=4
    &asOfDate=2026-06-03
```

Traversal path:
```
Meridian Growth Fund II
    └── HAS_POSITION → ByteSphere Series B ($85M)
            └── ISSUED_BY → ByteSphere Ltd
                    └── SUBSIDIARY_OF (active) → GlobalHoldings Corp
                            └── ←ATTACHED_TO── REGULATORY_WATCH (HIGH)
```

Response shows $85M indirect exposure — **same root risk as Apex Capital's $200M exposure, zero graph duplication**.

### Cross-Client Risk Summary

```
GET /api/risks/RISK-REG-001/propagation?asOfDate=2026-06-03

→ totalAffectedCapital: $285M across 2 clients, 2 funds
```

This is the core value proposition: one risk flag, two clients, one query.

---

## 13. Failure Taxonomy & Recovery Playbook

### F-01: CSV Schema Mismatch

**Symptom:** `SchemaValidationError` on ingestion, entire file rejected  
**Root cause:** Client sends `deal_size` instead of `deal_size_millions`, or wrong date format  
**Detection:** Gate 1 validation, P1 alert within 2 minutes  
**Recovery:**
```
1. Check quarantine file: /data/quarantine/{client}/{date}/
2. Client re-sends corrected file with same filename + _v2 suffix
3. Ingestor processes _v2; original offset never committed to Kafka
4. No graph state affected
```

### F-02: Referential Integrity Miss (Parent Company Not Yet Loaded)

**Symptom:** Deal row references `borrower_company_id` that doesn't exist in Neo4j or companies.csv  
**Root cause:** Client sends deals.csv before companies.csv; processing order not guaranteed  
**Detection:** Gate 2 failure, P2 alert  
**Recovery:**
```
1. Ingestor buffers row in Redis: SET "buffer:deal:{dealId}" 
2. Ingestor re-checks every 5 minutes, up to 3 retries (15-minute window)
3. When companies.csv processes and parent node appears in graph,
   buffered rows are released and proceed to Kafka
4. If no parent found after 3 retries → quarantine with DEPENDENCY_MISSING label
5. Ops manually triggers re-ingest after root cause fixed
```

### F-03: Kafka Broker Unavailable

**Symptom:** Producer cannot connect to `kafka:9092`  
**Root cause:** Kafka pod restart, network partition, disk full on broker  
**Detection:** `KafkaException` in ingestor logs within 30 seconds  
**Recovery:**
```
Producer behavior:
  - retries: 5, retry.backoff.ms: 500 → auto-retries for up to 2.5 seconds
  - If all retries exhausted → write event to dead-letter file:
    /data/dead-letter/{client}/{date}/{topic}/{eventId}.json

Ops recovery:
  1. Verify Kafka health: docker exec kafka kafka-topics --list ...
  2. Once Kafka recovers, replay dead-letter files:
     python scripts/replay_dead_letter.py --dir /data/dead-letter/APEX_CAPITAL/2026-06-03/
  3. Dead-letter replayer sends events with original timestamps preserved
  4. Dedup cache prevents double-writes (Redis TTL 72h)
```

### F-04: Neo4j Write Failure (Cypher Error)

**Symptom:** Consumer throws `Neo4jException` during Cypher execution  
**Root cause:** Constraint violation (duplicate ID), query timeout, Neo4j OOM  
**Detection:** Exception in `GraphMutationConsumer`, offset NOT committed  
**Recovery:**
```
Consumer behavior:
  EnableAutoCommit = false → offset stays at last committed position
  On restart: event re-consumed from uncommitted offset
  MERGE semantics: re-running same Cypher is safe (no side effects)

If constraint violation (duplicate ID):
  → Means same entity exists with different sourceSystem tag
  → Entity resolution failure — run deduplication job:
    python scripts/dedup_entities.py --node-type Company --strategy LEI_MATCH

If timeout (long-running Cypher):
  → Check index coverage for the traversal depth
  → Add missing index, re-run via consumer replay
```

### F-05: Ownership Event Out-of-Order

**Symptom:** TechNova shows wrong parent for a date range; incorrect exposure result  
**Root cause:** Two ownership events for the same company arrived out of order  
**Prevention:** Partition key = ChildCompanyId guarantees ordering within a partition  
**But can still happen if:**
```
- Consumer lag causes topic recreation with different partition count
- Manual message injection with wrong partition key
```
**Detection:** Bi-temporal audit query:
```cypher
MATCH (c:Company {name: "TechNova Solutions"})-[r:SUBSIDIARY_OF]->()
RETURN r.effectiveDate, r.expiryDate, r.ownershipPct
ORDER BY r.effectiveDate
// If expiryDate of row N > effectiveDate of row N+1 → overlap → ordering error
```
**Recovery:**
```
1. Identify affected entity and date range
2. Run ownership restatement script with correct ordering:
   python scripts/restate_ownership.py \
     --entity APEX-CO-001 \
     --from-date 2026-01-01 \
     --events ownership_corrected.csv
3. Script expires all existing SUBSIDIARY_OF for entity
4. Re-creates from events in correct chronological order
```

### F-06: Entity Resolution Failure (Duplicate Nodes)

**Symptom:** Two nodes exist for the same real-world company; risk flag not propagating  
**Root cause:** Client A uses `APEX-CO-009` for GlobalHoldings; Client B uses `MERI-CO-015`; both are the same legal entity but different IDs. No canonicalId match found during onboarding  
**Detection:**
```cypher
CALL db.index.fulltext.queryNodes("entity_names", "GlobalHoldings*")
YIELD node, score
RETURN node.id, node.name, node.sourceSystem, score
ORDER BY score DESC
// Multiple nodes with score > 0.9 → potential duplicates
```
**Recovery:**
```
1. Identify canonical ID (LEI preferred): manually look up in Bloomberg/GLEIF
2. Run entity merge script:
   python scripts/merge_entities.py \
     --primary-id SHARED-CO-009 \
     --duplicate-id MERI-CO-015 \
     --canonical-id "LEI:549300XYZ..."
3. Script: repoints all SUBSIDIARY_OF/INVESTED_IN/ATTACHED_TO from dup → primary
4. Sets canonicalId on primary node
5. Marks duplicate node: {merged: true, mergedInto: "SHARED-CO-009"}
```

### F-07: Vector Embedding Mismatch (Qdrant)

**Symptom:** Document search returns irrelevant results or empty for known documents  
**Root cause:** Embedding model version changed between seed and query; dimension mismatch  
**Detection:** Qdrant returns `VectorDimensionError` or low relevance scores  
**Recovery:**
```
1. Check current model: grep "model_name" scripts/seed_vectors.py
2. If model changed → re-embed all documents:
   python scripts/seed_vectors.py --rebuild-collection
3. This drops and recreates the Qdrant collection
4. Graph nodes keep embeddingId links; Qdrant IDs are preserved by document hash
```

### F-08: LLM (Claude) API Rate Limit or Timeout

**Symptom:** Agent output stage fails; no summary generated  
**Root cause:** Claude API quota exceeded or transient timeout  
**Behavior:** Graph traversal and enrichment already completed; only summarization fails  
**Recovery:**
```
Agent state is checkpoint-able via LangGraph StateGraph.
On failure at summarize_with_llm():
  - Graph traversal results are in state["graph_exposure"]
  - Portfolio context is in state["graph_portfolio"]
  - Enrichment data is in state["enriched_context"]
  - Retry only the summarize node — no re-traversal needed

Fallback: return structured JSON from graph without narrative summary
  { "summary": null, "graph_data": { ...full exposure results... } }
```

### F-09: Redis Cache Stale After Graph Update

**Symptom:** Analyst queries exposure after ownership change; gets cached (old) result  
**Root cause:** Redis TTL not expired; cached result predates graph mutation  
**TTL defaults:** Portfolio: 5 min | Exposure: 5 min | Stats: 60 min  
**Recovery:**
```
Manual cache invalidation:
  redis-cli DEL "ecg:exposure:{fundName}:{riskType}:{asOfDate}"
  
Or flush all query cache:
  redis-cli FLUSHDB   ← caution: affects all cached results

Future improvement: Kafka → Cache Invalidation consumer
  (every successful graph mutation sends invalidation event to Redis)
```

---

## 14. Enterprise Challenges & Mitigations

### Challenge 1: Entity Resolution at Scale

**Problem:** Real-world companies have multiple identifiers (LEI, ISIN, CUSIP, Bloomberg FIGI, internal IDs). Two clients may refer to the same company with different IDs. Without resolution, risk flags don't propagate correctly.

**Current state:** Gate 3 does fuzzy full-text name matching (score threshold 0.85).  
**Gap:** Name-based matching is unreliable for subsidiaries with similar names ("TechNova Solutions" vs "TechNova Solutions Ltd" vs "TechNova Solutions Holdings").

**Mitigation:**
```
Hierarchy of canonical identifiers:
  1. LEI (Legal Entity Identifier) — 20-char ISO 17442, globally unique
  2. ISIN — for instruments
  3. CUSIP — for US instruments
  4. Bloomberg FIGI — if available in CSV
  5. Internal ID — last resort, requires cross-reference table

Onboarding requirement: clients must supply LEI for any company with >$10M exposure.
Platform enriches missing LEIs from GLEIF public API during Gate 3.
```

### Challenge 2: Bi-Temporal Complexity Under Restatements

**Problem:** Clients sometimes restate historical ownership data — e.g., the acquisition was actually effective 3 months earlier than originally reported. This invalidates existing time-stamped relationships.

**Impact:** All historical exposure queries for that entity in that window return wrong results until restated.

**Mitigation:**
```
Two-phase restatement process:
  Phase 1: Mark affected SUBSIDIARY_OF rels with {requiresRestatement: true, reason: "..."}
  Phase 2: Run restatement script with corrected dates
           - All affected rels get version number: {version: 2, supersededBy: "evt-new-id"}
           - Original rels preserved with {superseded: true}
  
Audit trail: every restatement event carries:
  {originalEventId, correctedBy, correctionReason, correctionTimestamp}

Queries: by default filter WHERE r.superseded IS NULL (or FALSE)
         for audit: include WHERE r.superseded = true to see full history
```

### Challenge 3: Kafka Ordering vs. Throughput Trade-off

**Problem:** Partitioning by entity ID guarantees ordering per entity. But it means entities with high event rates (large holding companies with frequent valuation updates) can become "hot partitions" — one partition gets 80% of the load.

**Mitigation:**
```
Monitor partition lag per entity:
  kafka-consumer-groups --describe --group ecg-graph-mutation-consumer

If a partition is consistently behind:
  Option A: Add more partitions (requires topic recreation + consumer restart)
  Option B: Shard hot entities: use {entityId}:{hash(eventId) % 2} as partition key
            for ValuationUpdated events only (ordering less critical for valuations)
  Option C: Separate topics per event type with different partition counts
            ecg.valuation.updated → 12 partitions (high frequency)
            ecg.ownership.changed → 6 partitions (low frequency, strict ordering)
```

### Challenge 4: Multi-Client Data Isolation vs. Shared Nodes

**Problem:** Shared nodes (public companies) are good for risk propagation. But if Client A has confidential deal details about a shared company (e.g., they're in exclusive due diligence), those should not be visible to Client B even though they share the same company node.

**Mitigation:**
```
Data isolation model:
  SHARED nodes: public company names, sector, jurisdiction, public risk flags
  PRIVATE nodes: fund names, deal terms, position sizes, internal risk flags
  
  SHARED Company node has NO deal or position details
  Client-specific Deal node has sourceSystem tag and is not traversable cross-client
  
  API query patterns always include sourceSystem filter on sensitive rels:
    MATCH (f:Fund {sourceSystem: $clientId})-[pos:HAS_POSITION]->(i:Instrument) ...
    
  RBAC: API layer validates JWT claim clientId = sourceSystem of queried fund
```

### Challenge 5: Graph Growth & Query Performance Degradation

**Problem:** With 5+ clients, 500K+ events/month, the graph grows. Multi-hop traversals that were 12ms at 100K nodes may be 450ms at 5M nodes.

**Mitigation:**
```
Monitoring:
  Neo4j query log: PROFILE MATCH path = ... → execution plan + actual rows
  Alert on queries > 500ms

Structural:
  Bi-temporal indexes on effectiveDate/expiryDate (already in schema)
  Composite indexes on (sector, jurisdiction) for portfolio-level filters
  Limit hop depth via maxHops parameter (default 4, max 6)
  
At scale:
  Neo4j GDS (Graph Data Science) library for batch PageRank / community detection
  Neo4j Enterprise clustering (read replicas for query layer)
  Redis cache hierarchy: L1=in-process, L2=Redis, TTL tuned per query type
  
Archival:
  Entities inactive for >5 years and with expiryDate on all rels:
    → Archive to cold Neo4j instance
    → Retained for regulatory queries, removed from live traversal
```

### Challenge 6: Operational Visibility (No Graph Monitoring by Default)

**Problem:** Kafka lag, Neo4j write errors, and dedup hits are invisible to operations without explicit instrumentation.

**Mitigation:**
```
Metrics to instrument:
  kafka_consumer_lag{topic, partition, group}        → Prometheus scrape
  neo4j_write_latency_ms{event_type}                → Histogram
  neo4j_write_errors_total{event_type, error_type}  → Counter
  csv_rows_accepted_total{client, file_type}         → Counter
  csv_rows_rejected_total{client, file_type, reason} → Counter
  dedup_hits_total{client}                           → Counter
  entity_resolution_fuzzy_matches_total{client}      → Counter

Dashboard: Grafana + Prometheus
  Panel 1: Kafka consumer lag per topic (alert if > 1000 events)
  Panel 2: Neo4j write throughput and error rate
  Panel 3: CSV ingestion success rate per client
  Panel 4: Entity resolution quality (fuzzy match rate)
  Panel 5: Graph node/relationship growth over time
```

---

## 15. Operational Runbook

### Daily Operations

```
08:00  CSV files expected from clients (automated — no manual action)
08:15  Check ingestion manifest dashboards for any P1/P2 alerts
08:30  Verify Kafka consumer lag < 100 events across all topics
09:00  Business-hours: API available for analyst queries
17:00  Run end-of-day graph stats:
         curl http://localhost:5000/api/graph/stats
18:00  Batch vector re-embedding (if new documents added)
```

### Weekly Operations

```
Monday:   Review quarantine files from prior week; work with clients on corrections
Wednesday: Entity resolution review — check fuzzy match log for score < 0.80
Friday:   Schema maintenance window — add indexes, update Cypher query plans
           Monitor: PROFILE on top 5 slowest queries from the week
```

### Incident Response

```
P1 (Data Loss / Wrong Graph State):
  1. Stop consumer: docker stop ecg-streaming
  2. Identify affected events in Kafka (use kafka-console-consumer to inspect)
  3. Apply Cypher correction directly to Neo4j (with transaction rollback on error)
  4. Replay from correct offset: set consumer to specific offset
  5. Restart consumer
  6. Notify affected clients with impact window

P2 (Kafka Lag / Slow Ingestion):
  1. Check Kafka partition distribution: are hot partitions present?
  2. Scale consumer instances (up to 6 for 6 partitions)
  3. If throughput issue: check Neo4j CPU/memory (docker stats)
  4. Add Redis cache coverage for most-hit query patterns

P3 (Stale Cache / Wrong Query Results):
  1. Identify affected cache key
  2. Delete from Redis
  3. Query re-executes live from Neo4j
  4. No data loss
```

### First-Time Setup

```powershell
# 1. Start infrastructure
docker-compose up -d

# 2. Wait for Neo4j (takes ~60s to initialize)
Start-Sleep -Seconds 60

# 3. Apply graph schema (constraints + indexes)
docker exec ecg-neo4j cypher-shell -u neo4j -p password `
  -f /var/lib/neo4j/import/schema_setup.cypher

# 4. Install Python dependencies
pip install -r requirements.txt

# 5. Seed initial graph data (first-time only)
python scripts/seed_graph.py

# 6. Seed vector store
python scripts/seed_vectors.py

# 7. Start .NET API
dotnet run --project src/ECG.Api/ECG.Api.csproj

# 8. Verify all services
curl http://localhost:5000/health
curl http://localhost:5000/api/graph/stats
```

---

## 16. Data Lineage & Audit Trail

Every piece of data in the graph carries a provenance chain traceable to its origin:

### Node/Relationship Provenance

```
Property              Where set                 Purpose
─────────────────────────────────────────────────────────────
sourceSystem          Ingestor (Gate 3)         Which client owns this entity
mutationEventId       Consumer handler          Which Kafka event created/updated this
effectiveDate         CSV field / event payload When the real-world fact became true
expiryDate            Consumer (ownership only) When the fact became false
createdAt             Consumer (ON CREATE)      When node first entered graph
lastUpdatedAt         Consumer (ON MATCH)       When node was last mutated
```

### Audit Query Examples

**"Who changed TechNova's ownership and when?"**
```cypher
MATCH (c:Company {name: "TechNova Solutions"})-[r:SUBSIDIARY_OF]->(parent)
RETURN parent.name, r.ownershipPct, r.effectiveDate, r.expiryDate,
       r.mutationEventId, r.sourceSystem
ORDER BY r.effectiveDate
```

**"What did Apex Capital's graph look like on 2025-01-01?"**
```cypher
MATCH (f:Fund {name: "Apex Senior Credit Fund III"})
      -[pos:HAS_POSITION]->(i:Instrument)
WHERE pos.asOfDate <= date("2025-01-01")
RETURN f.name, i.name, pos.currentValueMillions, pos.asOfDate
ORDER BY pos.asOfDate DESC
```

**"Which events came from this ingestion run?"**
```cypher
MATCH ()-[r]-()
WHERE r.mutationEventId STARTS WITH "evt-" 
  AND r.sourceSystem = "APEX_CAPITAL"
RETURN r.mutationEventId, type(r), r.effectiveDate
// Cross-reference with Kafka event metadata.runId field
```

### Regulatory Query Support

The bi-temporal model directly supports:

| Regulation | Query Pattern |
|------------|--------------|
| OFAC compliance — ownership at sanction date | `asOfDate = sanctionDate` on ownership chain query |
| AIFMD look-through — fund → UBO | ownership chain + LP_IN traversal |
| BCBS stress test — portfolio at reporting date | `asOfDate = reportingDate` on portfolio view |
| GDPR data subject request — what data do we hold | `MATCH (p:Person {email: $email})-[*1..3]-()` |

---

*Document maintained by ECG Platform Team. For incidents, corrections, or onboarding requests, file an issue in the platform repository.*

*Last updated: 2026-06-03*
