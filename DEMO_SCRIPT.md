# ECG Platform — Demo Script

> Step-by-step commands and talking points for a technical demo.
> Expected total run time: 20-25 minutes.

---

## Setup (before the demo — run these ahead of time)

```bash
# 1. Start all infrastructure
docker-compose up -d

# 2. Wait for Neo4j to be ready (~60 seconds)
docker-compose logs -f neo4j | grep "Started"

# 3. Apply graph schema
docker exec -i ecg-neo4j cypher-shell -u neo4j -p ecg_password123 < queries/schema_setup.cypher

# 4. Seed realistic data
pip install -r requirements.txt
python scripts/seed_graph.py

# 5. Load document vectors
python scripts/seed_vectors.py

# 6. (Optional) Start the API locally for faster dev iteration
cd src/ECG.Api && dotnet run
```

---

## Section 1: The Graph as Contextual Spine (~5 min)

### Step 1.1 — Open Neo4j Browser
```
URL: http://localhost:7474
User: neo4j
Pass: ecg_password123
```

**Run in Neo4j Browser:**
```cypher
// Show the entire graph (small dataset, renders nicely)
MATCH (n)-[r]->(m)
WHERE NOT n:Document
RETURN n, r, m LIMIT 150
```

**Talking point:**
> "This is the contextual spine. Every node is a real capital markets entity. Every relationship has provenance — a sourceSystem and an effectiveDate. The graph is not a search index; it's the structural truth of our portfolio."

---

### Step 1.2 — Show the Ownership Chain (before API is running)
```cypher
// Trace TechNova's full corporate ownership chain
MATCH path = (c:Company {name: 'TechNova Solutions'})
             -[:SUBSIDIARY_OF*1..6]->(parent:Company)
RETURN [n IN nodes(path) | n.name] AS chain, length(path) AS depth
ORDER BY depth ASC
```

**Expected output:**
```
chain: ['TechNova Solutions', 'Continental Group', 'GlobalHoldings Corp']
depth: 2
```

**Talking point:**
> "TechNova is two ownership hops from GlobalHoldings Corp. GlobalHoldings has a REGULATORY_WATCH flag. Without the graph, you'd need to cross-reference 3 different systems to discover this relationship."

---

### Step 1.3 — Show the 4-Hop Exposure Path
```cypher
// The key query: Fund → Instrument → Issuer → Chain → Risk
MATCH path = (f:Fund {name: 'Apex Senior Credit Fund III'})
             -[:HAS_POSITION]->(i:Instrument)
             -[:ISSUED_BY]->(c:Company)
             -[:SUBSIDIARY_OF*1..4]->(p:Company)
             <-[:ATTACHED_TO]-(r:Risk)
WHERE r.riskType = 'REGULATORY_WATCH' AND r.isActive = true
RETURN f.name AS fund, [n IN nodes(path) | n.name] AS exposurePath,
       length(path) AS hops, r.severity AS severity
ORDER BY hops ASC LIMIT 3
```

**Expected output (approximate):**
```
fund: "Apex Senior Credit Fund III"
exposurePath: ["Apex Senior Credit Fund III", "TechNova TLA", "TechNova Solutions",
               "Continental Group", "GlobalHoldings Corp"]
hops: 4
severity: "HIGH"
```

**Talking point:**
> "The fund holds TechNova's term loan. TechNova is owned by Continental Group, which is owned by GlobalHoldings Corp. GlobalHoldings has a HIGH severity REGULATORY_WATCH. That's 4 hops. No analyst would find this without graph traversal — and they definitely wouldn't find all 15 variations across all funds in the portfolio."

---

## Section 2: The REST API (~5 min)

### Step 2.1 — Multi-Hop Exposure Endpoint
```bash
curl -s "http://localhost:5000/api/funds/Apex%20Senior%20Credit%20Fund%20III/exposure?riskType=REGULATORY_WATCH&maxHops=4" | python -m json.tool
```

**Expected response structure:**
```json
{
  "fund": "Apex Senior Credit Fund III",
  "riskType": "REGULATORY_WATCH",
  "maxHops": 4,
  "exposureCount": 3,
  "exposureChains": [
    {
      "fund": "Apex Senior Credit Fund III",
      "directIssuer": "TechNova Solutions",
      "riskBearingEntity": "GlobalHoldings Corp",
      "totalHops": 4,
      "severity": "HIGH",
      "positionValueMillions": 200.0,
      "exposurePath": ["Apex Senior Credit Fund III", "TechNova TLA", "TechNova Solutions",
                       "Continental Group", "GlobalHoldings Corp"]
    }
  ]
}
```

**Talking point:**
> "The API is a thin layer over parameterized Cypher. All queries are parameterized — no string interpolation, no injection risk. The response includes the full traversal path so the frontend can render the ownership chain visually."

---

### Step 2.2 — Ownership Chain Endpoint
```bash
curl -s "http://localhost:5000/api/companies/TechNova%20Solutions/ownership-chain" | python -m json.tool
```

**Talking point:**
> "This is the UBO lookup — Ultimate Beneficial Owner. The compliance team asks this every time a new investment is reviewed. With a relational database, this requires 4+ self-joins. With the graph, it's a single variable-length pattern match."

---

### Step 2.3 — Risk Propagation Endpoint
```bash
curl -s "http://localhost:5000/api/risks/risk-rw-global/propagation" | python -m json.tool
```

**Expected response:**
```json
{
  "riskId": "risk-rw-global",
  "affectedEntities": 8,
  "affectedFunds": 4,
  "totalExposureMillions": 680.0,
  "propagation": [...]
}
```

**Talking point:**
> "A new REGULATORY_WATCH flag appears on GlobalHoldings Corp at 9 AM. By 9:01 AM, this endpoint can tell every portfolio manager which of their positions are indirectly exposed. That's the blast radius query. It's how we replace a 3-day analyst task with a 200ms API call."

---

### Step 2.4 — Graph Stats
```bash
curl -s "http://localhost:5000/api/graph/stats" | python -m json.tool
```

---

## Section 3: Real-Time Streaming (~5 min)

### Step 3.1 — Open Neo4j Browser in one window, terminal in another

**Neo4j Browser — run this live query (refresh manually):**
```cypher
MATCH (r:Risk)-[:ATTACHED_TO]->(c:Company)
WHERE r.isActive = true
RETURN r.riskType, r.severity, c.name
ORDER BY r.flaggedDate DESC
LIMIT 10
```

### Step 3.2 — Start Kafka producer in terminal
```bash
python scripts/kafka_producer.py --delay-ms 800
```

**Watch as the terminal prints events:**
```
[01/50] DealCreated    deal=deal-stream-0000-abc123
   ✓  [ecg.deal.created] partition=2 offset=0 key=deal-stream-0000-abc123

[16/50] OwnershipChanged child=co-technova → parent=co-global
   ✓  [ecg.ownership.changed] partition=0 offset=0 key=co-technova

[31/50] RiskFlagged    type=REGULATORY_WATCH sev=HIGH on=co-continental
   ✓  [ecg.risk.flagged] partition=3 offset=0 key=co-continental
```

**Talking point:**
> "Partition key is the canonical entity ID, not the event ID. This ensures that two concurrent restructuring events for the same company are always processed in order. Two events for different companies can process in parallel across partitions."

### Step 3.3 — Verify the OwnershipChanged updated the graph bi-temporally
```cypher
// Show all SUBSIDIARY_OF relationships for a company — active and historical
MATCH (c:Company {name: 'TechNova Solutions'})-[r:SUBSIDIARY_OF]->(p:Company)
RETURN c.name AS child, p.name AS parent,
       r.ownershipPct AS pct, r.effectiveDate AS effective, r.expiryDate AS expiry
ORDER BY r.effectiveDate ASC
```

**Talking point:**
> "You can see both the old and new relationships. The old one has an expiryDate — it's the historical record. The new one has expiryDate = null, meaning it's currently active. We never delete. This is the bi-temporal model."

---

## Section 4: Federation Gateway (~3 min)

### Step 4.1 — GraphQL Query (Banana Cake Pop UI)
```
URL: http://localhost:5001/graphql
```

**Run this query:**
```graphql
query FundIntelligence {
  fundIntelligence(
    fundName: "Apex Senior Credit Fund III"
    asOfDate: "2024-06-01"
  ) {
    fundName
    aumMillions
    positionCount
    positions {
      issuer
      instrumentType
      currentValueMillions
      worstRiskSeverity
    }
    exposureChains {
      riskBearingEntity
      severity
      totalHops
      exposurePath
    }
    relatedDocuments {
      title
      docType
      relevanceScore
    }
    queryMetadata {
      graphQueryMs
      dataSources
    }
  }
}
```

**Talking point:**
> "One GraphQL query returns graph structure, related documents from the vector store, and financial metrics from the warehouse — all merged. The graph ran first to get canonical entity IDs. Then Qdrant and the warehouse were called in parallel. The graph is the spine; everything else hangs off it."

---

## Section 5: Agent Intelligence Layer (~5 min)

### Step 5.1 — Run the LangGraph agent
```bash
export ANTHROPIC_API_KEY=your_key_here
python scripts/ecg_agent.py --fund "Apex Senior Credit Fund III" --risk REGULATORY_WATCH
```

**Expected output (abbreviated):**
```
[GRAPH] Traversing exposure chains: fund=Apex Senior Credit Fund III risk=REGULATORY_WATCH
[GRAPH] Found 3 exposure paths

[GRAPH] Fetching portfolio context...
[GRAPH] Portfolio: 8 positions, 2 at HIGH risk

[CONTEXT] Context assembled: 3 chains, 2 affected entities

[LLM] Generating risk narrative from graph results...

EXECUTIVE SUMMARY:
  Graph analysis identified 3 indirect REGULATORY_WATCH exposure paths for
  Apex Senior Credit Fund III, with 2 HIGH severity findings totalling $400M...

ACTION ITEMS:
  1. Review 3 indirect exposure paths with deal team
  2. Escalate 2 HIGH-severity findings to Risk Committee
  ...
```

**Talking point:**
> "Notice the architecture. The LangGraph workflow has 4 steps. Steps 1-3 are pure graph queries — no LLM. Step 4 is the only LLM call, and it's explicitly told: 'Do not infer relationships. Only explain what the graph found.' The LLM is a communication layer, not a knowledge source. The graph is the source of truth."

---

## Architecture Walkthrough (ADRs — 2 min)

Point to [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md) and walk through any ADR that comes up:

- **ADR-001**: "We chose Neo4j over Neptune because of GDS support and local dev ergonomics."
- **ADR-002**: "Every SUBSIDIARY_OF relationship has effectiveDate and expiryDate. Ownership changes expire the old relationship, create a new one. We never delete."
- **ADR-003**: "Graph runs first. It returns canonical entity IDs. Only then do we call Qdrant and the warehouse in parallel."
- **ADR-004**: "Partition key is entity ID. This guarantees causal ordering for same-entity events across Kafka partitions."
- **ADR-005**: "The LLM does not traverse the graph or infer relationships. It explains what the graph found."

---

## Troubleshooting

| Issue | Fix |
|---|---|
| Neo4j not starting | `docker-compose logs neo4j` — check memory; set `NEO4J_server_memory_heap_max__size: 2G` |
| "No exposure chains found" | Run `python scripts/seed_graph.py` to seed data |
| Kafka consumer lag | Check `docker-compose logs ecg-kafka` — topic may not be created yet |
| ECG API 503 | API waits for Neo4j health check; give it 60s after `docker-compose up` |
| Python `ImportError` | `pip install -r requirements.txt` |
