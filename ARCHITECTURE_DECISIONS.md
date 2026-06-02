# Architecture Decision Records — ECG Platform

> Each ADR captures the context, decision, and trade-offs for a key architectural choice.
> Status: **Accepted** unless noted otherwise.

---

## ADR-001: Graph Database Selection — Neo4j over Amazon Neptune

### Status: Accepted

### Context
The ECG Platform requires multi-hop traversal across complex entity hierarchies:
- Corporate ownership chains (up to 6 hops deep)
- Fund → Instrument → Issuer → Parent → Risk traversal paths
- Time-point queries ("what was the ownership structure on date X?")

We evaluated three options:
1. **Amazon Neptune** (graph-native, managed AWS service)
2. **Neo4j 5.x** (graph-native, self-hosted or cloud)
3. **PostgreSQL with recursive CTEs** (relational with ltree or recursive queries)

### Decision
**Neo4j 5.x** was selected.

### Rationale
| Criterion | Neo4j | Neptune | PostgreSQL |
|---|---|---|---|
| Cypher query language | ✅ Expressive, industry standard | ⚠ Gremlin/openCypher, less ergonomic | ❌ SQL+CTEs, verbose for hops |
| Variable-length path query | ✅ `[:REL*1..6]` native syntax | ✅ Supported | ⚠ Recursive CTEs, no path objects |
| GDS (graph algorithms) | ✅ PageRank, Betweenness built-in | ❌ No native GDS | ❌ Extension required |
| APOC (utility library) | ✅ Full APOC support | ❌ Not available | N/A |
| Local development | ✅ Docker image, 1-command start | ❌ Requires AWS account | ✅ Simple |
| Full-text + fuzzy search | ✅ Built-in fulltext index | ⚠ Limited | ✅ pg_trgm |
| Temporal relationship queries | ✅ Relationship property indexes | ⚠ Limited optimization | ⚠ Requires custom indexing |

Neptune was ruled out because: (1) it requires AWS infrastructure even for local dev, increasing developer friction; (2) it lacks native GDS which we need for PageRank-based systemic importance scoring; (3) Gremlin's imperative traversal style is harder to read/audit than Cypher's declarative pattern matching.

PostgreSQL was ruled out because: recursive CTEs do not return path objects, making it difficult to extract the ordered traversal sequence needed for exposure path reporting.

### Consequences
- **Accepted:** Neo4j Community edition is free; Enterprise edition requires a license for production HA.
- **Accepted:** Self-hosting adds operational overhead vs. fully managed Neptune.
- **Accepted:** Neo4j's Java-based runtime has higher memory requirements than lightweight relational stores.
- **Mitigated:** Docker Compose provides reproducible local environments. Production would use Neo4j AuraDB (managed cloud).

---

## ADR-002: Bi-Temporal Relationship Model for Ownership History

### Status: Accepted

### Context
Corporate ownership structures change constantly through acquisitions, divestitures, and restructurings. Naive approaches simply update or delete the old relationship, losing the historical record. Two requirements drove this decision:
1. **Regulatory audit**: "What was TechNova's ownership chain as of the OFAC inquiry date (2023-11-01)?"
2. **Backtesting**: "Which funds would have had exposure to this entity 6 months ago?"

### Decision
All `SUBSIDIARY_OF` relationships carry two temporal properties:
- `effectiveDate`: The real-world date this relationship became true
- `expiryDate`: The real-world date this relationship ceased to be true (`null` = currently active)

**Relationships are NEVER deleted.** When ownership changes:
1. `SET oldRel.expiryDate = event.effectiveDate` (expire old)
2. `CREATE newRel` with `effectiveDate = event.effectiveDate, expiryDate = null` (create new)

Time-point queries filter with:
```cypher
WHERE rel.effectiveDate <= $asOfDate
  AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate)
```

### Consequences
- **Accepted:** Relationship count grows over time (old rels are never deleted). For typical M&A activity (~10 changes/year per entity), this adds negligible storage.
- **Accepted:** Every traversal query must include the bi-temporal filter predicate. Enforced at the service layer in `ExposureQueryService`.
- **Benefit:** Complete ownership history with zero additional storage design complexity.
- **Benefit:** The Kafka consumer's `HandleOwnershipChangedAsync` implements the expire-and-create pattern atomically, ensuring no gap in coverage.

---

## ADR-003: Graph as Federation Spine — Graph Runs First

### Status: Accepted

### Context
The ECG Federation layer aggregates data from three sources:
- **Neo4j** — entity relationships (structural)
- **Qdrant** — document embeddings (semantic)
- **Financial Data Warehouse** — metrics (financial)

An alternative design runs all three in parallel, using entity name as the join key.

### Decision
**Neo4j always runs first.** The graph query returns canonical entity IDs that are then used to query Qdrant and the warehouse in parallel.

```
Request
  │
  ▼
[1] Neo4j graph traversal (serial — blocking)
  │  Returns: canonical entity IDs + structural paths
  │
  ├──▶ [2a] Qdrant similarity search (parallel)
  └──▶ [2b] Warehouse metrics fetch (parallel)
  │
  ▼
Merged Response
```

### Rationale
Running all three in parallel requires all sources to agree on entity identity. In practice:
- The warehouse uses internal company codes
- Qdrant payload uses entity_id from the graph seed
- External APIs may use ISIN or CUSIP

The graph is the **entity resolution layer** — it holds canonical IDs (`canonicalId` property) that all other systems reference. Without resolving identity first, parallel queries return inconsistent data.

Additionally, the graph query defines the *scope* of the enrichment: we only call the warehouse for entities that appear in the graph traversal result. This avoids unnecessary API calls for out-of-scope entities.

### Consequences
- **Accepted:** Latency = graph latency + max(Qdrant, warehouse) latency. In practice, graph traversal is 50-200ms, enrichment is 100-300ms, total P95 is under 500ms.
- **Accepted:** If Neo4j is unavailable, the entire federation request fails (by design — the graph is authoritative).
- **Benefit:** Consistent entity identity across all data sources.
- **Benefit:** Minimal unnecessary downstream API calls.

---

## ADR-004: Kafka Partition Key Strategy — Entity ID, Not Event ID

### Status: Accepted

### Context
Kafka guarantees ordering within a partition. For graph mutation events, two concurrent events affecting the same entity must be processed in order to maintain a consistent graph state.

Example of ordering violation: Two `OwnershipChangedEvent`s for the same company (C → A, then C → B) arrive on different partitions. If C → B is processed first, the graph briefly shows an incorrect state that the bi-temporal model cannot repair.

### Decision
**Partition key = canonical entity ID**, not event ID.

| Topic | Partition Key |
|---|---|
| `ecg.deal.created` | `DealId` |
| `ecg.deal.status.changed` | `DealId` |
| `ecg.ownership.changed` | `ChildCompanyId` |
| `ecg.risk.flagged` | `AttachedToEntityId` |
| `ecg.valuation.updated` | `InstrumentId` |

With 6 partitions, events for different entities are distributed in parallel. Events for the same entity always land on the same partition, guaranteeing causal ordering.

### Consequences
- **Accepted:** Hot partitions possible if one entity has disproportionately many events (e.g., during an acquisition). Mitigated by scaling consumers per partition.
- **Accepted:** Cannot rebalance partitions without replaying events (Kafka standard limitation).
- **Benefit:** Zero ordering violations for same-entity events.
- **Benefit:** Independent entities process in parallel — throughput scales with partition count.

---

## ADR-005: Multi-Hop Traversal is Graph-Native, Not LLM-Native

### Status: Accepted

### Context
Large language models (GPT-4, Claude) have broad world knowledge about corporate hierarchies, but their knowledge is:
1. Static (training cutoff)
2. Hallucination-prone for specific ownership percentages and regulatory flags
3. Not auditable — cannot cite a source for "TechNova is 78.5% owned by Continental Group"

An alternative agent design would ask the LLM "which entities own TechNova?" and use its response to drive graph queries.

### Decision
**The graph is the only authoritative source for structural relationships.** The LLM is a communication layer only.

```
CORRECT:  Graph traversal → LLM summarizes graph output
INCORRECT: LLM infers relationships → Graph validates LLM output
```

In `ecg_agent.py`, the agent prompt explicitly instructs:
> "Do NOT infer relationships — only explain what the graph has found."

### Rationale
- **Auditability**: Every relationship in the graph has `sourceSystem` and `effectiveDate`. The LLM has neither.
- **Accuracy**: An ownership percentage of 78.5% is not general knowledge — it comes from legal filings and must be exact for regulatory purposes.
- **Latency**: A graph traversal returning 15 paths takes 100ms. An LLM inference chain over the same question takes 3-8 seconds with higher variance.
- **Cost**: Graph queries have predictable cost ($0 marginal per query). LLM inference costs scale with token count.

### Consequences
- **Accepted:** The LLM adds latency only for the final summarization step. The structural work is always done by the graph.
- **Benefit:** Every AI-generated risk narrative can be traced back to a specific graph path, which can be audited.
- **Benefit:** The system remains accurate even if the LLM is replaced — the graph output is the canonical result, the LLM output is a formatted explanation.
