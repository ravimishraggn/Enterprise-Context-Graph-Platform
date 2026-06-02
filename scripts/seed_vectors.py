"""
ECG Platform — Vector Seed Script
Generates text embeddings for deal/company documents and loads them into Qdrant.

Each Qdrant point stores:
  - vector: 384-dim embedding from sentence-transformers
  - payload: entity_id, doc_type, title, entity_name, text_excerpt

Usage:
  pip install qdrant-client sentence-transformers
  python scripts/seed_vectors.py
"""

import uuid
import json
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
)
from sentence_transformers import SentenceTransformer

QDRANT_HOST = "localhost"
QDRANT_PORT = 6333
COLLECTION  = "ecg_documents"
VECTOR_DIM  = 384  # all-MiniLM-L6-v2 output dimension

# ─────────────────────────────────────────────────────────────────────────────
# Document corpus — realistic capital markets text snippets
# entity_id matches Company/Deal IDs from seed_graph.py
# ─────────────────────────────────────────────────────────────────────────────
DOCUMENTS = [
    {
        "entity_id": "co-technova",
        "doc_type": "CIM",
        "title": "TechNova Solutions — Confidential Information Memorandum",
        "entity_name": "TechNova Solutions",
        "text": """
        TechNova Solutions is a leading provider of enterprise workflow automation software
        serving Fortune 500 clients across financial services, healthcare, and manufacturing.
        The company generated $312M in ARR in FY2023 with 94% gross retention and 118% NRR.
        EBITDA margins of 32% reflect strong unit economics and a capital-light SaaS model.
        The proposed transaction finances a strategic acquisition of CloudEdge Analytics,
        expanding TechNova's data integration capabilities. Pro-forma leverage of 4.2x
        EBITDA is supported by 40%+ FCF conversion and a resilient subscription revenue base.
        """
    },
    {
        "entity_id": "co-technova",
        "doc_type": "ANNUAL_REPORT",
        "title": "TechNova Solutions — Annual Report 2023",
        "entity_name": "TechNova Solutions",
        "text": """
        FY2023 was a transformational year for TechNova. Total revenue grew 28% YoY to $389M.
        ARR reached $312M, up from $244M in FY2022. We expanded into APAC with the launch
        of TechNova Singapore and signed our largest enterprise contract to date — a $22M
        multi-year deal with a G-SIB. The board approved a $50M revolving credit facility
        increase to support continued international expansion. Headcount grew from 1,200 to 1,850.
        """
    },
    {
        "entity_id": "d-technova-lbo",
        "doc_type": "TERM_SHEET",
        "title": "TechNova LBO — Senior Term Sheet",
        "entity_name": "TechNova LBO",
        "text": """
        Senior Secured Term Loan A: $200M, L+275, 7-year maturity, 50% excess cash flow sweep.
        Senior Secured Term Loan B: $200M, L+325, 8-year maturity, 25% excess cash flow sweep.
        Revolving Credit Facility: $50M, L+250, 7-year maturity, 0.375% commitment fee.
        Financial covenants: Maximum Net Leverage 5.5x (stepping down to 4.5x by Year 3),
        Minimum Interest Coverage 2.0x. Security: First lien on all assets of TechNova Solutions
        and its material subsidiaries. Guarantors: All material domestic subsidiaries.
        """
    },
    {
        "entity_id": "co-medcore",
        "doc_type": "CIM",
        "title": "MedCore Devices Corp — Investment Memorandum",
        "entity_name": "MedCore Devices Corp",
        "text": """
        MedCore Devices is a leading European manufacturer of minimally invasive surgical
        instruments, with 68% market share in laparoscopic device accessories across DACH.
        The company generated €185M in revenue in FY2023, with EBITDA of €62M (33.5% margin).
        Products are CE-marked and FDA 510(k) cleared for US market entry, currently in pilot
        with two major US hospital groups. The proposed credit facility refinances €180M of
        existing senior debt at more favorable terms following rating upgrade to B+.
        """
    },
    {
        "entity_id": "co-swiftlog",
        "doc_type": "COVENANT_REPORT",
        "title": "SwiftLogistics Q1 2024 Covenant Compliance Report",
        "entity_name": "SwiftLogistics Group",
        "text": """
        COVENANT BREACH NOTICE — Q1 2024
        Net Total Leverage: 7.2x (Covenant Maximum: 6.5x) — BREACH
        Interest Coverage: 1.8x (Covenant Minimum: 2.0x) — BREACH

        Management Commentary: The covenant breaches are attributable to a €45M one-time
        restructuring charge related to the integration of AquaFreight GmbH, acquired in
        October 2023. Excluding restructuring, adjusted EBITDA of €142M yields 5.9x leverage.
        Management has initiated waiver discussions with the lending group. A forbearance
        agreement is expected by end of Q2 2024. Operational cash flow remains positive at €28M.
        """
    },
    {
        "entity_id": "co-global",
        "doc_type": "ANNUAL_REPORT",
        "title": "GlobalHoldings Corp — Regulatory Filing 2023",
        "entity_name": "GlobalHoldings Corp",
        "text": """
        GlobalHoldings Corp is a diversified holding company with interests spanning
        technology, industrials, and consumer sectors across 18 countries.
        As disclosed in our regulatory filings, the company is currently engaged with
        OFAC regarding the classification of certain subsidiary activities in connection
        with our Eastern European operations. We have retained external counsel and
        are cooperating fully with all regulatory inquiries. The Board's Risk Committee
        meets monthly to review our compliance posture. No material financial impact is
        currently anticipated, though we cannot rule out restrictions on certain asset transfers.
        """
    },
    {
        "entity_id": "co-euro",
        "doc_type": "LEGAL_OPINION",
        "title": "EuroGroup Holdco — ECB AML Assessment Response",
        "entity_name": "EuroGroup Holdco",
        "text": """
        This legal opinion is issued in connection with the Luxembourg Financial Intelligence
        Unit (FIU) enhanced due diligence request dated January 2024. EuroGroup Holdco
        has submitted a comprehensive response addressing beneficial ownership disclosures,
        source of funds documentation, and transaction monitoring protocols. Our AML
        compliance programme has been independently assessed as 'Satisfactory' by Deloitte.
        We anticipate resolution of the enhanced monitoring status within 90 days, subject
        to satisfactory review of the supplemental documentation submitted on 15 March 2024.
        """
    },
    {
        "entity_id": "co-healthplus",
        "doc_type": "CIM",
        "title": "HealthPlus Services — Acquisition Financing Memorandum",
        "entity_name": "HealthPlus Services",
        "text": """
        HealthPlus Services is a leading multi-site outpatient healthcare provider operating
        245 clinics across 28 US states. The company provides primary care, specialist
        referral, and chronic disease management services, with 60% of revenue from
        value-based care contracts. FY2023 revenue of $420M with EBITDA of $84M (20% margin).
        Patient volume grew 15% YoY, driven by Medicare Advantage expansion and de novo
        clinic openings. The LBO financing of $355M supports acquisition of NovaCare
        Clinics, adding 42 sites in the Southeast with immediate EBITDA contribution.
        """
    },
    {
        "entity_id": "co-cloudbase",
        "doc_type": "CIM",
        "title": "CloudBase Software — Series B Growth Equity Memorandum",
        "entity_name": "CloudBase Software",
        "text": """
        CloudBase delivers a unified cloud infrastructure management platform used by
        2,400 enterprise customers to optimize multi-cloud spending. The company achieved
        $89M ARR in FY2023 (105% NRR) with gross margins of 78%. The $125M Series B
        will fund: (1) expansion of the AI-powered optimization engine, (2) EMEA GTM
        build-out (18 enterprise AEs), and (3) ISV partnership programme. Comparable
        transactions in cloud FinOps: Apptio acquired at 12x ARR (2023), Harness raised
        at 15x ARR. CloudBase projects $180M ARR by FY2025, targeting profitability at scale.
        """
    },
    {
        "entity_id": "co-primeenergy",
        "doc_type": "ANNUAL_REPORT",
        "title": "PrimeEnergy Assets — 2023 Annual Reserves Report",
        "entity_name": "PrimeEnergy Assets Inc",
        "text": """
        PrimeEnergy Assets holds producing oil and gas interests across the Permian Basin
        and Mid-continent regions. Total proved reserves of 42.3 MMBoe as of 31 Dec 2023,
        with PV-10 value of $680M at strip pricing. FY2023 production averaged 9,800 Boepd,
        up 12% from 8,750 Boepd in FY2022. Revenue of $195M and EBITDA of $122M reflect
        strong WTI realizations averaging $78/bbl. The Company's reserve-based lending
        facility provides $180M of borrowing capacity, supporting continued development
        of the Wolfcamp formation in our Midland Basin acreage position.
        """
    },
]


def main():
    print(f"\n{'='*60}")
    print("ECG Platform — Vector Seed Script")
    print(f"Qdrant: {QDRANT_HOST}:{QDRANT_PORT}")
    print(f"Collection: {COLLECTION}")
    print(f"{'='*60}\n")

    # ── Load embedding model ─────────────────────────────────────────────────
    print("Loading sentence-transformer model (all-MiniLM-L6-v2)...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    print(f"   Model loaded. Vector dimension: {VECTOR_DIM}\n")

    # ── Connect to Qdrant ────────────────────────────────────────────────────
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    # Create or recreate collection
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION in collections:
        print(f"Collection '{COLLECTION}' exists — deleting and recreating...")
        client.delete_collection(COLLECTION)

    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
    )
    print(f"Collection '{COLLECTION}' created.\n")

    # ── Generate embeddings and upsert ───────────────────────────────────────
    print("Generating embeddings and upserting to Qdrant...")
    points = []
    for i, doc in enumerate(DOCUMENTS):
        text = doc["text"].strip()
        embedding = model.encode(text).tolist()

        point = PointStruct(
            id=str(uuid.uuid4()),
            vector=embedding,
            payload={
                "entity_id":    doc["entity_id"],
                "doc_type":     doc["doc_type"],
                "title":        doc["title"],
                "entity_name":  doc["entity_name"],
                "text_excerpt": text[:500],  # store excerpt for display
            }
        )
        points.append(point)
        print(f"   [{i+1}/{len(DOCUMENTS)}] {doc['title'][:60]}...")

    client.upsert(collection_name=COLLECTION, points=points)

    print(f"\n✓  {len(points)} document vectors loaded into Qdrant.")

    # ── Verify with a test query ─────────────────────────────────────────────
    test_query = "regulatory watch sanctioned jurisdictions holding company"
    test_vec   = model.encode(test_query).tolist()
    results    = client.search(
        collection_name=COLLECTION,
        query_vector=test_vec,
        limit=3,
        with_payload=True
    )
    print(f"\nVerification — query: '{test_query}'")
    for r in results:
        print(f"   score={r.score:.3f}  entity={r.payload['entity_id']}  "
              f"title={r.payload['title'][:50]}")

    print("\n✓  Qdrant vector store ready.\n")


if __name__ == "__main__":
    main()
