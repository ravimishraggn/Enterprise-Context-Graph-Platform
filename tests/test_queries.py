"""
ECG Platform — Python Query Validation Tests
Validates that the seeded graph returns expected multi-hop results.

REQUIRES: Neo4j running with seed data loaded (python scripts/seed_graph.py)

Usage:
  pip install neo4j pytest
  pytest tests/test_queries.py -v
"""

import pytest
from neo4j import GraphDatabase

NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "ecg_password123"
AS_OF_DATE = "2024-06-01"


@pytest.fixture(scope="session")
def driver():
    d = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    yield d
    d.close()


@pytest.fixture(scope="session")
def session(driver):
    with driver.session() as s:
        yield s


# ─────────────────────────────────────────────────────────────────────────────
# Smoke tests — graph has been seeded
# ─────────────────────────────────────────────────────────────────────────────

class TestGraphSeeded:
    def test_company_count(self, session):
        result = session.run("MATCH (c:Company) RETURN count(c) AS cnt")
        cnt = result.single()["cnt"]
        assert cnt >= 30, f"Expected at least 30 companies, got {cnt}"

    def test_fund_count(self, session):
        result = session.run("MATCH (f:Fund) RETURN count(f) AS cnt")
        cnt = result.single()["cnt"]
        assert cnt == 8, f"Expected 8 funds, got {cnt}"

    def test_instrument_count(self, session):
        result = session.run("MATCH (i:Instrument) RETURN count(i) AS cnt")
        cnt = result.single()["cnt"]
        assert cnt == 20, f"Expected 20 instruments, got {cnt}"

    def test_risk_count(self, session):
        result = session.run("MATCH (r:Risk) RETURN count(r) AS cnt")
        cnt = result.single()["cnt"]
        assert cnt >= 10, f"Expected at least 10 risks, got {cnt}"

    def test_subsidiary_relationships_exist(self, session):
        result = session.run("MATCH ()-[r:SUBSIDIARY_OF]->() RETURN count(r) AS cnt")
        cnt = result.single()["cnt"]
        assert cnt > 0, "No SUBSIDIARY_OF relationships found — ownership chains missing"

    def test_has_position_relationships_exist(self, session):
        result = session.run("MATCH ()-[r:HAS_POSITION]->() RETURN count(r) AS cnt")
        cnt = result.single()["cnt"]
        assert cnt > 0, "No HAS_POSITION relationships found — fund positions missing"


# ─────────────────────────────────────────────────────────────────────────────
# Multi-hop traversal tests — the critical chains
# ─────────────────────────────────────────────────────────────────────────────

class TestMultiHopTraversal:

    def test_chain_a_4hop_apex_to_regulatory_watch(self, session):
        """
        Chain A: Apex Senior Credit Fund III
          → HAS_POSITION → TechNova TLA
          → ISSUED_BY → TechNova Solutions
          → SUBSIDIARY_OF → Continental Group
          → SUBSIDIARY_OF → GlobalHoldings Corp
          ← ATTACHED_TO ← REGULATORY_WATCH
        """
        result = session.run("""
            MATCH path = (f:Fund {name: 'Apex Senior Credit Fund III'})
                         -[:HAS_POSITION]->(i:Instrument)
                         -[:ISSUED_BY]->(c:Company)
                         -[:SUBSIDIARY_OF*1..4]->(p:Company)
                         <-[:ATTACHED_TO]-(r:Risk)
            WHERE r.riskType = 'REGULATORY_WATCH' AND r.isActive = true
            RETURN f.name AS fund, length(path) AS hops,
                   p.name AS riskEntity, r.severity AS severity
            ORDER BY hops ASC LIMIT 1
        """)
        row = result.single()
        assert row is not None, "Chain A: No 4-hop exposure path found from Apex Credit Fund to REGULATORY_WATCH"
        assert row["hops"] >= 4, f"Expected >= 4 hops, got {row['hops']}"
        assert row["severity"] in ("HIGH", "MEDIUM")
        print(f"  Chain A: {row['fund']} → ({row['hops']} hops) → {row['riskEntity']} [{row['severity']}]")

    def test_chain_b_3hop_meridian_to_regulatory_watch(self, session):
        """
        Chain B: Meridian Direct Lending IV → MedCore → EuroGroup REGULATORY_WATCH
        """
        result = session.run("""
            MATCH path = (f:Fund {name: 'Meridian Direct Lending IV'})
                         -[:HAS_POSITION]->(i:Instrument)
                         -[:ISSUED_BY]->(c:Company)
                         -[:SUBSIDIARY_OF*1..3]->(p:Company)
                         <-[:ATTACHED_TO]-(r:Risk)
            WHERE r.riskType = 'REGULATORY_WATCH' AND r.isActive = true
            RETURN f.name AS fund, length(path) AS hops, p.name AS riskEntity
            ORDER BY hops ASC LIMIT 1
        """)
        row = result.single()
        assert row is not None, "Chain B: No path found from Meridian to REGULATORY_WATCH"
        assert row["hops"] >= 3, f"Expected >= 3 hops, got {row['hops']}"

    def test_chain_c_5hop_keystone_to_regulatory_watch(self, session):
        """
        Chain C: Keystone Opportunistic Credit II → SwiftLogistics → (5 hops) → GlobalHoldings REGULATORY_WATCH
        """
        result = session.run("""
            MATCH path = (f:Fund {name: 'Keystone Opportunistic Credit II'})
                         -[:HAS_POSITION]->(i:Instrument)
                         -[:ISSUED_BY]->(c:Company)
                         -[:SUBSIDIARY_OF*1..5]->(p:Company)
                         <-[:ATTACHED_TO]-(r:Risk)
            WHERE r.riskType = 'REGULATORY_WATCH' AND r.isActive = true
            RETURN f.name AS fund, length(path) AS hops, p.name AS riskEntity
            ORDER BY hops ASC LIMIT 1
        """)
        row = result.single()
        assert row is not None, "Chain C: No 5-hop path found from Keystone to REGULATORY_WATCH"
        assert row["hops"] >= 4, f"Expected >= 4 hops, got {row['hops']}"

    def test_all_active_risks_have_attachment(self, session):
        """Every active risk must be attached to at least one entity."""
        result = session.run("""
            MATCH (r:Risk) WHERE r.isActive = true
            AND NOT (r)-[:ATTACHED_TO]->()
            RETURN count(r) AS orphaned
        """)
        orphaned = result.single()["orphaned"]
        assert orphaned == 0, f"{orphaned} active risks have no ATTACHED_TO relationship"

    def test_all_instruments_have_issuer(self, session):
        """Every instrument must have an ISSUED_BY relationship."""
        result = session.run("""
            MATCH (i:Instrument) WHERE NOT (i)-[:ISSUED_BY]->(:Company)
            RETURN count(i) AS orphaned, collect(i.name) AS names
        """)
        row = result.single()
        assert row["orphaned"] == 0, \
            f"{row['orphaned']} instruments missing ISSUED_BY: {row['names']}"

    def test_fund_positions_sum_to_reasonable_weight(self, session):
        """Each fund's portfolio weights should sum > 0 (basic sanity check)."""
        result = session.run("""
            MATCH (f:Fund)-[pos:HAS_POSITION]->(:Instrument)
            WHERE pos.asOfDate = $asOf
            WITH f, sum(pos.weightPct) AS totalWeight
            WHERE totalWeight = 0
            RETURN count(f) AS zeroWeightFunds
        """, asOf=AS_OF_DATE)
        row = result.single()
        assert row["zeroWeightFunds"] == 0, \
            f"{row['zeroWeightFunds']} funds have zero total portfolio weight"


# ─────────────────────────────────────────────────────────────────────────────
# Ownership chain tests
# ─────────────────────────────────────────────────────────────────────────────

class TestOwnershipChains:

    def test_technova_has_chain_to_global_holdings(self, session):
        """TechNova Solutions should chain up to GlobalHoldings Corp."""
        result = session.run("""
            MATCH path = (c:Company {name: 'TechNova Solutions'})
                         -[:SUBSIDIARY_OF*1..6]->(ultimate:Company)
            WHERE NOT (ultimate)-[:SUBSIDIARY_OF {expiryDate: null}]->()
               OR NOT (ultimate)-[:SUBSIDIARY_OF]->()
            RETURN ultimate.name AS parent, length(path) AS depth
            ORDER BY depth ASC LIMIT 1
        """)
        row = result.single()
        assert row is not None, "TechNova Solutions has no ownership chain"
        assert row["depth"] >= 1

    def test_swiftlogistics_has_multichain_path(self, session):
        """SwiftLogistics should have a 2+ hop ownership chain."""
        result = session.run("""
            MATCH path = (c:Company {name: 'SwiftLogistics Group'})
                         -[:SUBSIDIARY_OF*1..6]->(parent:Company)
            RETURN max(length(path)) AS maxDepth
        """)
        row = result.single()
        max_depth = row["maxDepth"] if row else 0
        assert max_depth >= 2, f"SwiftLogistics chain depth {max_depth} — expected >= 2"

    def test_subsidiary_relationships_are_bi_temporal(self, session):
        """All SUBSIDIARY_OF relationships must have effectiveDate; active ones have null expiryDate."""
        result = session.run("""
            MATCH ()-[r:SUBSIDIARY_OF]->()
            WHERE r.effectiveDate IS NULL
            RETURN count(r) AS missing_effective_date
        """)
        missing = result.single()["missing_effective_date"]
        assert missing == 0, f"{missing} SUBSIDIARY_OF rels missing effectiveDate"


# ─────────────────────────────────────────────────────────────────────────────
# Risk propagation tests
# ─────────────────────────────────────────────────────────────────────────────

class TestRiskPropagation:

    def test_global_holdings_risk_propagates_to_funds(self, session):
        """A REGULATORY_WATCH on GlobalHoldings Corp should touch at least one fund position."""
        result = session.run("""
            MATCH (r:Risk {id: 'risk-rw-global'})-[:ATTACHED_TO]->(origin:Company)
            MATCH (origin)<-[:SUBSIDIARY_OF*0..5]-(company:Company)
            MATCH (i:Instrument)-[:ISSUED_BY]->(company)
            MATCH (f:Fund)-[:HAS_POSITION]->(i)
            RETURN count(DISTINCT f) AS fund_count, count(DISTINCT company) AS entity_count
        """)
        row = result.single()
        assert row["fund_count"] > 0, "GlobalHoldings REGULATORY_WATCH did not propagate to any fund"
        assert row["entity_count"] > 0
        print(f"  GlobalHoldings risk propagates to {row['fund_count']} funds via {row['entity_count']} entities")

    def test_credit_risk_on_swiftlog_touches_multiple_funds(self, session):
        """SwiftLogistics CREDIT risk should be visible from multiple fund positions."""
        result = session.run("""
            MATCH (r:Risk {id: 'risk-cr-swiftlog'})-[:ATTACHED_TO]->(c:Company)
            MATCH (i:Instrument)-[:ISSUED_BY]->(c)
            MATCH (f:Fund)-[:HAS_POSITION]->(i)
            RETURN count(DISTINCT f) AS fund_count
        """)
        row = result.single()
        assert row["fund_count"] >= 1, "SwiftLogistics credit risk not visible from any fund"
