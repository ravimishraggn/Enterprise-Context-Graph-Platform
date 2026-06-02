// =============================================================================
// ECG Platform — Neo4j Schema Setup
// Run this ONCE against a fresh database before seeding data.
// All constraints and indexes use IF NOT EXISTS for idempotency.
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// NODE UNIQUENESS CONSTRAINTS
// These also implicitly create a B-tree index on the constrained property.
// ─────────────────────────────────────────────────────────────────────────────
CREATE CONSTRAINT company_id IF NOT EXISTS
    FOR (c:Company) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT fund_id IF NOT EXISTS
    FOR (f:Fund) REQUIRE f.id IS UNIQUE;

CREATE CONSTRAINT deal_id IF NOT EXISTS
    FOR (d:Deal) REQUIRE d.id IS UNIQUE;

CREATE CONSTRAINT instrument_id IF NOT EXISTS
    FOR (i:Instrument) REQUIRE i.id IS UNIQUE;

CREATE CONSTRAINT investor_id IF NOT EXISTS
    FOR (inv:Investor) REQUIRE inv.id IS UNIQUE;

CREATE CONSTRAINT risk_id IF NOT EXISTS
    FOR (r:Risk) REQUIRE r.id IS UNIQUE;

CREATE CONSTRAINT document_id IF NOT EXISTS
    FOR (doc:Document) REQUIRE doc.id IS UNIQUE;

CREATE CONSTRAINT person_id IF NOT EXISTS
    FOR (p:Person) REQUIRE p.id IS UNIQUE;

// ─────────────────────────────────────────────────────────────────────────────
// RANGE INDEXES — support fast equality + range lookups used in traversal
// ─────────────────────────────────────────────────────────────────────────────

// Entity resolution: canonical ID lookup across source systems
CREATE INDEX company_canonical IF NOT EXISTS
    FOR (c:Company) ON (c.canonicalId);

// Sector-based filtering on portfolio views
CREATE INDEX company_sector IF NOT EXISTS
    FOR (c:Company) ON (c.sector);

// Jurisdiction-based regulatory queries
CREATE INDEX company_jurisdiction IF NOT EXISTS
    FOR (c:Company) ON (c.jurisdiction);

CREATE INDEX company_type IF NOT EXISTS
    FOR (c:Company) ON (c.companyType);

// Deal pipeline / status filtering
CREATE INDEX deal_status IF NOT EXISTS
    FOR (d:Deal) ON (d.status);

CREATE INDEX deal_type IF NOT EXISTS
    FOR (d:Deal) ON (d.dealType);

// Risk propagation queries filter by type + severity constantly
CREATE INDEX risk_type IF NOT EXISTS
    FOR (r:Risk) ON (r.riskType);

CREATE INDEX risk_severity IF NOT EXISTS
    FOR (r:Risk) ON (r.severity);

CREATE INDEX risk_active IF NOT EXISTS
    FOR (r:Risk) ON (r.isActive);

// Instrument type + maturity for portfolio analytics
CREATE INDEX instrument_type IF NOT EXISTS
    FOR (i:Instrument) ON (i.instrumentType);

CREATE INDEX instrument_maturity IF NOT EXISTS
    FOR (i:Instrument) ON (i.maturityDate);

// Fund status + type for portfolio-level queries
CREATE INDEX fund_status IF NOT EXISTS
    FOR (f:Fund) ON (f.status);

CREATE INDEX fund_type IF NOT EXISTS
    FOR (f:Fund) ON (f.fundType);

// ─────────────────────────────────────────────────────────────────────────────
// COMPOSITE INDEXES — support compound filter predicates
// ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX risk_type_severity_active IF NOT EXISTS
    FOR (r:Risk) ON (r.riskType, r.severity, r.isActive);

CREATE INDEX company_sector_jurisdiction IF NOT EXISTS
    FOR (c:Company) ON (c.sector, c.jurisdiction);

// ─────────────────────────────────────────────────────────────────────────────
// FULL-TEXT INDEX — entity name resolution (fuzzy / partial matching)
// Used by the entity resolution service to match inbound names to canonical IDs
// ─────────────────────────────────────────────────────────────────────────────

CREATE FULLTEXT INDEX entity_name_search IF NOT EXISTS
    FOR (c:Company|f:Fund|d:Deal|i:Instrument)
    ON EACH [c.name, f.name, d.dealName, i.name];

// ─────────────────────────────────────────────────────────────────────────────
// RELATIONSHIP PROPERTY INDEXES
// Neo4j 5.x supports indexes on relationship properties for bi-temporal queries.
// The SUBSIDIARY_OF effectiveDate / expiryDate are queried on every traversal.
// ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX subsidiary_effective IF NOT EXISTS
    FOR ()-[r:SUBSIDIARY_OF]-() ON (r.effectiveDate);

CREATE INDEX subsidiary_expiry IF NOT EXISTS
    FOR ()-[r:SUBSIDIARY_OF]-() ON (r.expiryDate);

CREATE INDEX position_as_of IF NOT EXISTS
    FOR ()-[r:HAS_POSITION]-() ON (r.asOfDate);

// ─────────────────────────────────────────────────────────────────────────────
// VERIFY — list all constraints and indexes
// ─────────────────────────────────────────────────────────────────────────────
// SHOW CONSTRAINTS;
// SHOW INDEXES;
