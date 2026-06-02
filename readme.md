# Enterprise Context Graph (ECG) Platform

A production-grade graph intelligence platform for capital markets. The graph is the **contextual spine** — every exposure query, every risk analysis, and every agent workflow starts with graph traversal.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ECG Platform                                       │
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                 │
│  │  ECG.Api     │    │ ECG.Stream-  │    │  ECG.Federa- │                 │
│  │  .NET 8      │    │ ing          │    │  tion        │                 │
│  │  Minimal API │    │ Kafka        │    │  GraphQL     │                 │
│  │  :5000       │    │ Consumer     │    │  Gateway     │                 │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                 │
│         │                   │                   │                          │
│         └───────────────────┼───────────────────┘                          │
│                             │                                               │
│                    ┌────────▼────────┐                                      │
│                    │   ECG.Graph     │     ┌───────────────────────────┐   │
│                    │   Neo4j Driver  │────▶│  Neo4j 5.x (Graph DB)     │   │
│                    │   Repository    │     │  APOC + GDS plugins       │   │
│                    │   + Query Svc   │     │  Bolt :7687 / HTTP :7474  │   │
│                    └─────────────────┘     └───────────────────────────┘   │
│                                                                             │
│  ┌───────────────┐  ┌───────────────┐  ┌────────────────┐                 │
│  │    Kafka      │  │    Qdrant     │  │     Redis      │                 │
│  │    :9092      │  │    :6333      │  │     :6379      │                 │
│  │  5 ECG topics │  │  doc vectors  │  │  result cache  │                 │
│  └───────────────┘  └───────────────┘  └────────────────┘                 │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │   Python Scripts                                                    │   │
│  │   seed_graph.py | seed_vectors.py | kafka_producer.py | ecg_agent  │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Graph Node Types

| Label | Description | Key Relationships |
|---|---|---|
| `Company` | Legal entity (borrower, issuer, sponsor, holding) | `SUBSIDIARY_OF` (ownership chain) |
| `Fund` | Investment fund (credit, PE, hybrid, mezz) | `HAS_POSITION`, `INVESTED_IN` |
| `Instrument` | Debt/equity instrument | `ISSUED_BY`, `FINANCED_BY` |
| `Deal` | Capital markets transaction | `INVOLVES`, `FINANCED_BY` |
| `Risk` | Risk flag (credit, regulatory, operational) | `ATTACHED_TO` |
| `Investor` | LP or co-investor | `LP_IN` |
| `Person` | Board member or executive | `SERVES_ON_BOARD_OF` |
| `Document` | CIM, term sheet, annual report | `RELATES_TO` |

### Key Relationship: SUBSIDIARY_OF (Bi-Temporal)

```
(TechNova Solutions)─[SUBSIDIARY_OF {ownershipPct:100, effectiveDate:'2021-09-15', expiryDate:null}]
  ─▶ (Continental Group)─[SUBSIDIARY_OF {ownershipPct:78.5, effectiveDate:'2019-01-01', expiryDate:null}]
    ─▶ (GlobalHoldings Corp) ◀─[ATTACHED_TO]─ (Risk: REGULATORY_WATCH, severity:HIGH)
```

**Bi-temporal model:** Relationships are never deleted. When ownership changes, `expiryDate` is set on the old relationship and a new one is created. This enables time-travel queries.

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- .NET 8 SDK (for C# services)
- Python 3.10+ (for scripts)

### 1. Start Infrastructure

```bash
docker-compose up -d
```

Wait ~60 seconds for Neo4j to initialize:
```bash
docker-compose logs -f neo4j | grep "Started"
```

### 2. Initialize Graph Schema

```bash
docker exec -i ecg-neo4j cypher-shell -u neo4j -p ecg_password123 \
    < queries/schema_setup.cypher
```

### 3. Seed Data

```bash
pip install -r requirements.txt
python scripts/seed_graph.py
```

### 4. Build and Run the API

```bash
# Initialize .NET solution first (one time)
pwsh setup.ps1

# Run API
cd src/ECG.Api && dotnet run
```

### 5. Verify

```bash
# Graph stats
curl http://localhost:5000/api/graph/stats

# 4-hop exposure query
curl "http://localhost:5000/api/funds/Apex%20Senior%20Credit%20Fund%20III/exposure?riskType=REGULATORY_WATCH"

# Ownership chain
curl "http://localhost:5000/api/companies/TechNova%20Solutions/ownership-chain"

# Risk propagation blast radius
curl "http://localhost:5000/api/risks/risk-rw-global/propagation"
```

---

## API Reference

### `GET /api/funds/{fundName}/exposure`
Multi-hop indirect exposure analysis.

**Response:**
```json
{
  "fund": "Apex Senior Credit Fund III",
  "exposureCount": 3,
  "exposureChains": [{
    "directIssuer": "TechNova Solutions",
    "riskBearingEntity": "GlobalHoldings Corp",
    "totalHops": 4,
    "severity": "HIGH",
    "positionValueMillions": 200.0,
    "exposurePath": ["Apex Senior Credit Fund III", "TechNova TLA",
                     "TechNova Solutions", "Continental Group", "GlobalHoldings Corp"]
  }]
}
```

### `GET /api/funds/{fundName}/portfolio`
Full portfolio view with direct and indirect risk overlay.

### `GET /api/companies/{name}/ownership-chain`
Trace full corporate ownership chain to ultimate beneficial owner.

### `GET /api/risks/{riskId}/propagation`
Blast radius — all affected entities, instruments, and funds for a risk flag.

### `GET /api/risks/exposure?riskType=REGULATORY_WATCH`
All indirect exposure paths for a risk type across the entire portfolio.

### `GET /api/graph/stats`
Node and relationship counts by label.

---

## Real-Time Streaming

```bash
# Produce 50 events: 20 deals + 15 ownership changes + 15 risk flags
python scripts/kafka_producer.py

# Watch graph update in Neo4j Browser (http://localhost:7474)
```

**Partition key = canonical entity ID** (not event ID), ensuring causal ordering per entity.

---

## Agent Intelligence

```bash
export ANTHROPIC_API_KEY=<your_key>
python scripts/ecg_agent.py --fund "Apex Senior Credit Fund III" --risk REGULATORY_WATCH
```

**Architecture:**
- Steps 1-3: Pure graph traversal (no LLM)
- Step 4: LLM summarizes graph output — does NOT infer relationships

---

## Bi-Temporal Model Explained

Every `SUBSIDIARY_OF` relationship carries:
- `effectiveDate` — when this ownership became true in the real world
- `expiryDate` — when it ended (`null` = currently active)

Relationships are **never deleted**. Ownership changes expire the old relationship and create a new one, enabling time-travel queries:

```cypher
// What was the structure on 2023-01-01?
MATCH (c:Company)-[r:SUBSIDIARY_OF]->(p:Company)
WHERE r.effectiveDate <= '2023-01-01'
  AND (r.expiryDate IS NULL OR r.expiryDate > '2023-01-01')
RETURN c.name, p.name, r.ownershipPct
```

---

## Why Graph vs. Relational

| Query | SQL | Cypher |
|---|---|---|
| Find subsidiaries at any depth | Recursive CTE, brittle at depth 5+ | `[:SUBSIDIARY_OF*1..6]` |
| Fund → Instrument → Chain → Risk | 4+ self-joins | Single pattern match |
| Return traversal path | Not supported | `[n IN nodes(path)]` |
| Time-point ownership | Complex date-ranged self-join | Relationship property filter |

---

## Documentation

- [DEMO_SCRIPT.md](DEMO_SCRIPT.md) — Step-by-step demo with expected output and talking points
- [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md) — 5 ADRs: Neo4j selection, bi-temporal model, federation pattern, Kafka partitioning, graph vs. LLM roles
