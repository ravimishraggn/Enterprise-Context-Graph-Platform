"""
ECG Platform — Graph Seed Script
Seeds Neo4j with realistic capital markets data:
  - 30 Company nodes (sponsors, portfolio companies, holding companies, subsidiaries)
  - 8  Fund nodes
  - 15 Deal nodes
  - 20 Instrument nodes
  - 10 Investor nodes
  - 10 Risk nodes
  - 5  Person nodes
  - All relationships with bi-temporal metadata

OWNERSHIP CHAINS (enable multi-hop traversal):
  Chain A (4 hops): Apex Credit Fund III → TechNova TLA → TechNova Solutions
                    → Continental Group → GlobalHoldings Corp ← REGULATORY_WATCH
  Chain B (3 hops): Meridian Direct Lending IV → MedCore Senior Note → MedCore Devices
                    → EuroGroup Holdco ← CREDIT risk
  Chain C (5 hops): Keystone Opp Credit II → SwiftLogistics TLB → SwiftLogistics Group
                    → SwiftLogistics Europe → EuroGroup Holdco
                    → GlobalHoldings Corp ← REGULATORY_WATCH

Usage:
  pip install neo4j
  python scripts/seed_graph.py
  python scripts/seed_graph.py --wipe   # Clear all data first
"""

import argparse
import uuid
from datetime import datetime, timedelta
from neo4j import GraphDatabase

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "ecg_password123"
AS_OF_DATE = "2024-06-01"

def uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

def now_str() -> str:
    return datetime.utcnow().isoformat()

# ─────────────────────────────────────────────────────────────────────────────
# Node data
# ─────────────────────────────────────────────────────────────────────────────

# ── PE Sponsors (5) ──────────────────────────────────────────────────────────
SPONSORS = [
    {"id": "co-apex",     "name": "Apex Capital Partners",    "companyType": "SPONSOR", "sector": "Financial Services", "jurisdiction": "US"},
    {"id": "co-meridian", "name": "Meridian Credit Group",    "companyType": "SPONSOR", "sector": "Financial Services", "jurisdiction": "US"},
    {"id": "co-keystone", "name": "Keystone Lending Partners","companyType": "SPONSOR", "sector": "Financial Services", "jurisdiction": "US"},
    {"id": "co-atlas",    "name": "Atlas Investment Partners", "companyType": "SPONSOR", "sector": "Financial Services", "jurisdiction": "US"},
    {"id": "co-crown",    "name": "Crown Strategic Capital",  "companyType": "SPONSOR", "sector": "Financial Services", "jurisdiction": "GB"},
]

# ── Portfolio Companies / Borrowers (10) ─────────────────────────────────────
PORTFOLIO_COS = [
    {"id": "co-technova",  "name": "TechNova Solutions",        "companyType": "BORROWER",  "sector": "Technology",          "jurisdiction": "US"},
    {"id": "co-medcore",   "name": "MedCore Devices Corp",      "companyType": "BORROWER",  "sector": "Healthcare",          "jurisdiction": "DE"},
    {"id": "co-retailedge","name": "RetailEdge Holdings",       "companyType": "BORROWER",  "sector": "Consumer Discretionary","jurisdiction": "US"},
    {"id": "co-primeenergy","name":"PrimeEnergy Assets Inc",    "companyType": "BORROWER",  "sector": "Energy",              "jurisdiction": "US"},
    {"id": "co-swiftlog",  "name": "SwiftLogistics Group",      "companyType": "BORROWER",  "sector": "Industrials",         "jurisdiction": "NL"},
    {"id": "co-cloudbase", "name": "CloudBase Software",        "companyType": "BORROWER",  "sector": "Technology",          "jurisdiction": "US"},
    {"id": "co-precmfg",   "name": "PrecisionMfg Corp",         "companyType": "BORROWER",  "sector": "Industrials",         "jurisdiction": "DE"},
    {"id": "co-healthplus", "name":"HealthPlus Services",       "companyType": "BORROWER",  "sector": "Healthcare",          "jurisdiction": "US"},
    {"id": "co-mediastream","name":"MediaStream Group",         "companyType": "BORROWER",  "sector": "Communication Services","jurisdiction": "GB"},
    {"id": "co-finpayments","name":"FinPayments Platform",      "companyType": "BORROWER",  "sector": "Financial Services",  "jurisdiction": "SG"},
]

# ── Holding / Parent Companies (5) ────────────────────────────────────────────
HOLDING_COS = [
    {"id": "co-global",    "name": "GlobalHoldings Corp",       "companyType": "HOLDING",   "sector": "Diversified",         "jurisdiction": "US"},
    {"id": "co-euro",      "name": "EuroGroup Holdco",          "companyType": "HOLDING",   "sector": "Diversified",         "jurisdiction": "LU"},
    {"id": "co-pacific",   "name": "Pacific Rim Holdings",      "companyType": "HOLDING",   "sector": "Diversified",         "jurisdiction": "SG"},
    {"id": "co-continental","name": "Continental Group",        "companyType": "HOLDING",   "sector": "Diversified",         "jurisdiction": "US"},
    {"id": "co-finservgrp","name": "FinancialServices Group",   "companyType": "HOLDING",   "sector": "Financial Services",  "jurisdiction": "GB"},
]

# ── Intermediate Subsidiaries (10) ────────────────────────────────────────────
SUBSIDIARIES = [
    {"id": "co-technova-us",   "name": "TechNova US Holdings",       "companyType": "HOLDING", "sector": "Technology",    "jurisdiction": "US"},
    {"id": "co-medcore-eu",    "name": "MedCore Europe BV",           "companyType": "HOLDING", "sector": "Healthcare",    "jurisdiction": "NL"},
    {"id": "co-swiftlog-eu",   "name": "SwiftLogistics Europe SARL",  "companyType": "HOLDING", "sector": "Industrials",   "jurisdiction": "FR"},
    {"id": "co-retail-us",     "name": "RetailEdge US OpCo",          "companyType": "HOLDING", "sector": "Consumer Discretionary","jurisdiction": "US"},
    {"id": "co-energy-opco",   "name": "PrimeEnergy OpCo LLC",        "companyType": "HOLDING", "sector": "Energy",        "jurisdiction": "US"},
    {"id": "co-cloud-eu",      "name": "CloudBase Technologies EU",   "companyType": "HOLDING", "sector": "Technology",    "jurisdiction": "IE"},
    {"id": "co-media-us",      "name": "MediaStream US Inc",          "companyType": "HOLDING", "sector": "Communication Services","jurisdiction": "US"},
    {"id": "co-fin-apac",      "name": "FinPayments APAC Pte Ltd",    "companyType": "HOLDING", "sector": "Financial Services","jurisdiction": "SG"},
    {"id": "co-precmfg-asia",  "name": "PrecisionMfg Asia Ltd",       "companyType": "HOLDING", "sector": "Industrials",   "jurisdiction": "CN"},
    {"id": "co-health-eu",     "name": "HealthPlus EU GmbH",          "companyType": "HOLDING", "sector": "Healthcare",    "jurisdiction": "DE"},
]

ALL_COMPANIES = SPONSORS + PORTFOLIO_COS + HOLDING_COS + SUBSIDIARIES

# ── Funds (8) ─────────────────────────────────────────────────────────────────
FUNDS = [
    {"id": "f-apex-scf3",  "name": "Apex Senior Credit Fund III",       "fundType": "CREDIT",   "vintage": 2021, "aumMillions": 2100, "manager": "Apex Capital Partners", "status": "INVESTING"},
    {"id": "f-merid-dl4",  "name": "Meridian Direct Lending IV",        "fundType": "CREDIT",   "vintage": 2022, "aumMillions": 3400, "manager": "Meridian Credit Group", "status": "INVESTING"},
    {"id": "f-key-opp2",   "name": "Keystone Opportunistic Credit II",  "fundType": "MEZZ",     "vintage": 2020, "aumMillions": 1800, "manager": "Keystone Lending Partners","status": "HARVESTING"},
    {"id": "f-atlas-cp6",  "name": "Atlas Capital Partners VI",         "fundType": "PE",       "vintage": 2021, "aumMillions": 4200, "manager": "Atlas Investment Partners","status": "INVESTING"},
    {"id": "f-crown-sp",   "name": "Crown Strategic Partners",          "fundType": "HYBRID",   "vintage": 2022, "aumMillions": 2800, "manager": "Crown Strategic Capital","status": "INVESTING"},
    {"id": "f-apex-dd1",   "name": "Apex Distressed Debt Fund I",       "fundType": "CREDIT",   "vintage": 2020, "aumMillions": 1200, "manager": "Apex Capital Partners", "status": "HARVESTING"},
    {"id": "f-merid-rec",  "name": "Meridian Real Estate Credit",       "fundType": "CREDIT",   "vintage": 2021, "aumMillions":  900, "manager": "Meridian Credit Group", "status": "INVESTING"},
    {"id": "f-key-infra",  "name": "Keystone Infrastructure Debt",      "fundType": "CREDIT",   "vintage": 2022, "aumMillions": 1500, "manager": "Keystone Lending Partners","status": "INVESTING"},
]

# ── Deals (15) ────────────────────────────────────────────────────────────────
DEALS = [
    {"id": "d-technova-lbo",  "dealName": "TechNova LBO",              "dealType": "LBO",             "dealSizeMillions": 450, "currency": "USD", "status": "CLOSED", "closeDate": "2021-09-15"},
    {"id": "d-medcore-cf",    "dealName": "MedCore Credit Facility",   "dealType": "CREDIT_FACILITY", "dealSizeMillions": 280, "currency": "EUR", "status": "CLOSED", "closeDate": "2022-03-01"},
    {"id": "d-retailedge-lbo","dealName": "RetailEdge LBO",            "dealType": "LBO",             "dealSizeMillions": 320, "currency": "USD", "status": "CLOSED", "closeDate": "2021-06-30"},
    {"id": "d-primeenergy-cf","dealName": "PrimeEnergy Reserve CF",    "dealType": "CREDIT_FACILITY", "dealSizeMillions": 180, "currency": "USD", "status": "CLOSED", "closeDate": "2022-11-20"},
    {"id": "d-swiftlog-lbo",  "dealName": "SwiftLogistics Acquisition","dealType": "LBO",             "dealSizeMillions": 550, "currency": "EUR", "status": "CLOSED", "closeDate": "2021-12-10"},
    {"id": "d-cloudbase-ge",  "dealName": "CloudBase Growth Equity",   "dealType": "GROWTH_EQUITY",   "dealSizeMillions": 125, "currency": "USD", "status": "CLOSED", "closeDate": "2022-07-15"},
    {"id": "d-precmfg-recap", "dealName": "PrecisionMfg Recapitalization","dealType":"RECAPITALIZATION","dealSizeMillions":220,"currency":"EUR","status":"CLOSED","closeDate":"2020-05-20"},
    {"id": "d-healthplus-lbo","dealName": "HealthPlus LBO",            "dealType": "LBO",             "dealSizeMillions": 390, "currency": "USD", "status": "CLOSED", "closeDate": "2022-02-28"},
    {"id": "d-mediastream-cf","dealName": "MediaStream Credit Facility","dealType":"CREDIT_FACILITY", "dealSizeMillions": 150, "currency": "GBP", "status": "CLOSED", "closeDate": "2021-08-01"},
    {"id": "d-finpay-ge",     "dealName": "FinPayments Series B",      "dealType": "GROWTH_EQUITY",   "dealSizeMillions":  95, "currency": "USD", "status": "PIPELINE","closeDate": None},
    {"id": "d-technova2-cf",  "dealName": "TechNova Add-On CF",        "dealType": "CREDIT_FACILITY", "dealSizeMillions": 100, "currency": "USD", "status": "PIPELINE","closeDate": None},
    {"id": "d-retailedge-exit","dealName":"RetailEdge Exit",           "dealType": "LBO",             "dealSizeMillions": 420, "currency": "USD", "status": "EXITED", "closeDate": "2023-11-30"},
    {"id": "d-primeenergy2-mezz","dealName":"PrimeEnergy Mezz Tranche","dealType":"MEZZ",             "dealSizeMillions": 75,  "currency": "USD", "status": "CLOSED", "closeDate": "2023-01-15"},
    {"id": "d-cloudbase2-ge", "dealName": "CloudBase Series C",        "dealType": "GROWTH_EQUITY",   "dealSizeMillions": 200, "currency": "USD", "status": "PIPELINE","closeDate": None},
    {"id": "d-healthplus-add","dealName": "HealthPlus Add-On Financing","dealType":"CREDIT_FACILITY", "dealSizeMillions": 120, "currency": "USD", "status": "CLOSED", "closeDate": "2023-06-01"},
]

# ── Instruments (20) ─────────────────────────────────────────────────────────
INSTRUMENTS = [
    # TechNova — 4-hop chain anchor
    {"id": "ins-tna-tla",  "name": "TechNova TLA",          "instrumentType": "TERM_LOAN_A",      "isin": "US87654321AB", "faceValueMillions": 200, "coupon": 0.0650, "maturityDate": "2028-09-15", "currency": "USD", "issuer": "co-technova"},
    {"id": "ins-tna-tlb",  "name": "TechNova TLB",          "instrumentType": "TERM_LOAN_B",      "isin": "US87654321CD", "faceValueMillions": 200, "coupon": 0.0700, "maturityDate": "2029-09-15", "currency": "USD", "issuer": "co-technova"},
    {"id": "ins-tna-rcf",  "name": "TechNova RCF",          "instrumentType": "REVOLVING_CREDIT", "isin": None,           "faceValueMillions":  50, "coupon": 0.0575, "maturityDate": "2027-09-15", "currency": "USD", "issuer": "co-technova"},

    # MedCore — 3-hop chain anchor
    {"id": "ins-mdc-snr",  "name": "MedCore Senior Note",   "instrumentType": "BOND",             "isin": "DE00012345AB", "faceValueMillions": 150, "coupon": 0.0625, "maturityDate": "2029-03-01", "currency": "EUR", "issuer": "co-medcore"},
    {"id": "ins-mdc-tla",  "name": "MedCore TLA",           "instrumentType": "TERM_LOAN_A",      "isin": "DE00012345CD", "faceValueMillions": 100, "coupon": 0.0580, "maturityDate": "2028-03-01", "currency": "EUR", "issuer": "co-medcore"},

    # RetailEdge
    {"id": "ins-ret-tla",  "name": "RetailEdge TLA",        "instrumentType": "TERM_LOAN_A",      "isin": "US11223344AB", "faceValueMillions": 120, "coupon": 0.0700, "maturityDate": "2027-06-30", "currency": "USD", "issuer": "co-retailedge"},
    {"id": "ins-ret-tlb",  "name": "RetailEdge TLB",        "instrumentType": "TERM_LOAN_B",      "isin": "US11223344CD", "faceValueMillions": 150, "coupon": 0.0750, "maturityDate": "2028-06-30", "currency": "USD", "issuer": "co-retailedge"},

    # PrimeEnergy
    {"id": "ins-pe-tla",   "name": "PrimeEnergy TLA",       "instrumentType": "TERM_LOAN_A",      "isin": "US99887766AB", "faceValueMillions":  90, "coupon": 0.0625, "maturityDate": "2028-11-20", "currency": "USD", "issuer": "co-primeenergy"},
    {"id": "ins-pe-mezz",  "name": "PrimeEnergy Mezz Note", "instrumentType": "MEZZ_NOTE",        "isin": "US99887766EF", "faceValueMillions":  40, "coupon": 0.1100, "maturityDate": "2030-01-15", "currency": "USD", "issuer": "co-primeenergy"},

    # SwiftLogistics — 5-hop chain anchor
    {"id": "ins-sl-tla",   "name": "SwiftLogistics TLA",    "instrumentType": "TERM_LOAN_A",      "isin": "NL00556677AB", "faceValueMillions": 180, "coupon": 0.0650, "maturityDate": "2028-12-10", "currency": "EUR", "issuer": "co-swiftlog"},
    {"id": "ins-sl-tlb",   "name": "SwiftLogistics TLB",    "instrumentType": "TERM_LOAN_B",      "isin": "NL00556677CD", "faceValueMillions": 280, "coupon": 0.0725, "maturityDate": "2029-12-10", "currency": "EUR", "issuer": "co-swiftlog"},

    # CloudBase
    {"id": "ins-cb-pref",  "name": "CloudBase Preferred",   "instrumentType": "EQUITY",           "isin": None,           "faceValueMillions":  75, "coupon": None,   "maturityDate": None,         "currency": "USD", "issuer": "co-cloudbase"},

    # PrecisionMfg
    {"id": "ins-pmfg-tla", "name": "PrecisionMfg TLA",      "instrumentType": "TERM_LOAN_A",      "isin": "DE00334455AB", "faceValueMillions": 120, "coupon": 0.0600, "maturityDate": "2027-05-20", "currency": "EUR", "issuer": "co-precmfg"},

    # HealthPlus
    {"id": "ins-hp-tla",   "name": "HealthPlus TLA",        "instrumentType": "TERM_LOAN_A",      "isin": "US44556677AB", "faceValueMillions": 175, "coupon": 0.0675, "maturityDate": "2029-02-28", "currency": "USD", "issuer": "co-healthplus"},
    {"id": "ins-hp-tlb",   "name": "HealthPlus TLB",        "instrumentType": "TERM_LOAN_B",      "isin": "US44556677CD", "faceValueMillions": 180, "coupon": 0.0725, "maturityDate": "2030-02-28", "currency": "USD", "issuer": "co-healthplus"},

    # MediaStream
    {"id": "ins-ms-snr",   "name": "MediaStream Senior Bond","instrumentType": "BOND",            "isin": "GB00112233AB", "faceValueMillions":  80, "coupon": 0.0625, "maturityDate": "2028-08-01", "currency": "GBP", "issuer": "co-mediastream"},

    # FinPayments
    {"id": "ins-fp-pref",  "name": "FinPayments Series B Pref","instrumentType":"EQUITY",         "isin": None,           "faceValueMillions":  60, "coupon": None,   "maturityDate": None,         "currency": "USD", "issuer": "co-finpayments"},

    # Additional high-quality assets for diversification
    {"id": "ins-hp-rcf",   "name": "HealthPlus RCF",        "instrumentType": "REVOLVING_CREDIT", "isin": None,           "faceValueMillions":  35, "coupon": 0.0575, "maturityDate": "2027-02-28", "currency": "USD", "issuer": "co-healthplus"},
    {"id": "ins-ret-snr",  "name": "RetailEdge Senior Note", "instrumentType": "BOND",            "isin": "US11223344EF", "faceValueMillions": 100, "coupon": 0.0750, "maturityDate": "2029-06-30", "currency": "USD", "issuer": "co-retailedge"},
    {"id": "ins-pmfg-rcf", "name": "PrecisionMfg RCF",      "instrumentType": "REVOLVING_CREDIT", "isin": None,           "faceValueMillions":  40, "coupon": 0.0550, "maturityDate": "2026-05-20", "currency": "EUR", "issuer": "co-precmfg"},
]

# ── Risks (10) ───────────────────────────────────────────────────────────────
RISKS = [
    # These 3 are attached to holding companies — they propagate down ownership chains
    {"id": "risk-rw-global",   "riskType": "REGULATORY_WATCH", "severity": "HIGH",   "description": "GlobalHoldings Corp is under OFAC review for subsidiary exposure in sanctioned jurisdictions. Potential asset freeze risk.", "flaggedDate": "2023-11-01", "isActive": True, "flaggedBy": "REGULATORY_FEED", "attachTo": "co-global"},
    {"id": "risk-rw-euro",     "riskType": "REGULATORY_WATCH", "severity": "HIGH",   "description": "EuroGroup Holdco subject to ECB enhanced due diligence following AML concerns raised by Luxembourg regulator.", "flaggedDate": "2024-01-15", "isActive": True, "flaggedBy": "REGULATORY_FEED", "attachTo": "co-euro"},
    {"id": "risk-rw-finserv",  "riskType": "REGULATORY_WATCH", "severity": "MEDIUM", "description": "FinancialServices Group under FCA investigation for market conduct issues in UK subsidiary.", "flaggedDate": "2024-03-10", "isActive": True, "flaggedBy": "ANALYST",         "attachTo": "co-finservgrp"},

    # Credit risks on portfolio companies
    {"id": "risk-cr-swiftlog", "riskType": "CREDIT",           "severity": "HIGH",   "description": "SwiftLogistics covenant breach on leverage ratio (actual: 7.2x vs 6.5x max). Waiver negotiation in progress.", "flaggedDate": "2024-02-28", "isActive": True, "flaggedBy": "SYSTEM",          "attachTo": "co-swiftlog"},
    {"id": "risk-cr-retailedge","riskType":"CREDIT",           "severity": "MEDIUM", "description": "RetailEdge EBITDA declining YoY due to consumer slowdown. DSC coverage has thinned to 1.05x.", "flaggedDate": "2024-04-01", "isActive": True, "flaggedBy": "ANALYST",         "attachTo": "co-retailedge"},
    {"id": "risk-cr-precmfg",  "riskType": "CREDIT",           "severity": "MEDIUM", "description": "PrecisionMfg raw material cost inflation compressing margins. Free cash flow negative in Q1 2024.", "flaggedDate": "2024-03-15", "isActive": True, "flaggedBy": "SYSTEM",          "attachTo": "co-precmfg"},

    # Operational risks on deals
    {"id": "risk-op-swlbo",    "riskType": "OPERATIONAL",      "severity": "LOW",    "description": "SwiftLogistics LBO integration delayed 3 months — IT systems consolidation behind schedule.", "flaggedDate": "2022-06-01", "isActive": True, "flaggedBy": "ANALYST",         "attachTo": "d-swiftlog-lbo"},
    {"id": "risk-op-hplbo",    "riskType": "OPERATIONAL",      "severity": "LOW",    "description": "HealthPlus LBO management team transition ongoing — CFO search underway.", "flaggedDate": "2023-09-01", "isActive": True, "flaggedBy": "ANALYST",         "attachTo": "d-healthplus-lbo"},

    # Market risks on instruments
    {"id": "risk-mkt-eurobond","riskType": "MARKET",           "severity": "MEDIUM", "description": "EUR/USD rate movement increasing USD-equivalent value of EUR-denominated debt. 8% FX headwind YTD.", "flaggedDate": "2024-01-10", "isActive": True, "flaggedBy": "MARKET_DATA",     "attachTo": "ins-mdc-snr"},
    {"id": "risk-mkt-floater", "riskType": "MARKET",           "severity": "MEDIUM", "description": "Floating rate instruments now at peak SOFR environment. Borrowers facing 200bps more than initial underwriting.", "flaggedDate": "2023-08-01", "isActive": True, "flaggedBy": "MARKET_DATA",     "attachTo": "ins-sl-tlb"},
]

# ── Investors (10) ────────────────────────────────────────────────────────────
INVESTORS = [
    {"id": "inv-pension-us",   "name": "NorthState Pension Fund",         "investorType": "LP",         "domicile": "US", "committedCapitalMillions": 500},
    {"id": "inv-endow-ivy",    "name": "Ivy University Endowment",        "investorType": "LP",         "domicile": "US", "committedCapitalMillions": 250},
    {"id": "inv-swf-gulf",     "name": "Gulf Sovereign Investment Fund",  "investorType": "ANCHOR",     "domicile": "AE", "committedCapitalMillions": 800},
    {"id": "inv-ins-eu",       "name": "EuroInsurance Asset Management",  "investorType": "LP",         "domicile": "DE", "committedCapitalMillions": 300},
    {"id": "inv-fof-uk",       "name": "Meridian Fund of Funds LP",       "investorType": "LP",         "domicile": "GB", "committedCapitalMillions": 200},
    {"id": "inv-family-us",    "name": "Harrington Family Office",        "investorType": "CO_INVESTOR","domicile": "US", "committedCapitalMillions": 150},
    {"id": "inv-pension-eu",   "name": "Netherlands Civil Pension",       "investorType": "ANCHOR",     "domicile": "NL", "committedCapitalMillions": 600},
    {"id": "inv-corp-us",      "name": "TechVentures Corporate LP",       "investorType": "CO_INVESTOR","domicile": "US", "committedCapitalMillions": 100},
    {"id": "inv-swf-asia",     "name": "Asia Pacific Growth Fund",        "investorType": "LP",         "domicile": "SG", "committedCapitalMillions": 400},
    {"id": "inv-ins-uk",       "name": "Britannia Life Insurance",        "investorType": "LP",         "domicile": "GB", "committedCapitalMillions": 350},
]

# ── Persons (5) ──────────────────────────────────────────────────────────────
PERSONS = [
    {"id": "p-ceo-technova",  "fullName": "Sarah Chen",      "role": "CEO",          "email": "schen@technova-corp.com",    "boardOf": "co-technova"},
    {"id": "p-cfo-swiftlog",  "fullName": "Marcus Bauer",    "role": "CFO",          "email": "mbauer@swiftlogistics.eu",   "boardOf": "co-swiftlog"},
    {"id": "p-ceo-global",    "fullName": "Victoria Strand", "role": "CEO",          "email": "vstrand@globalholdings.com", "boardOf": "co-global"},
    {"id": "p-rep-apex",      "fullName": "David Thornton",  "role": "SPONSOR_REP",  "email": "dthorn@apexcapital.com",     "boardOf": "co-technova"},
    {"id": "p-cfo-medcore",   "fullName": "Klaus Hoffman",   "role": "CFO",          "email": "khoffman@medcore-de.com",    "boardOf": "co-medcore"},
]

# ─────────────────────────────────────────────────────────────────────────────
# Seeding logic
# ─────────────────────────────────────────────────────────────────────────────

def seed(tx, wipe: bool = False):
    if wipe:
        print("⚠  Wiping all graph data...")
        tx.run("MATCH (n) DETACH DELETE n")
        print("   Cleared.")

    # ── Companies ────────────────────────────────────────────────────────────
    print("→ Seeding companies...")
    for c in ALL_COMPANIES:
        tx.run("""
            MERGE (co:Company {id: $id})
            SET co += {
                name:         $name,
                canonicalId:  $id,
                companyType:  $companyType,
                sector:       $sector,
                jurisdiction: $jurisdiction,
                isActive:     true,
                sourceSystem: 'MANUAL',
                createdAt:    $ts,
                updatedAt:    $ts
            }
        """, id=c["id"], name=c["name"], companyType=c["companyType"],
             sector=c["sector"], jurisdiction=c["jurisdiction"], ts=now_str())
    print(f"   {len(ALL_COMPANIES)} companies seeded.")

    # ── Funds ────────────────────────────────────────────────────────────────
    print("→ Seeding funds...")
    for f in FUNDS:
        tx.run("""
            MERGE (fn:Fund {id: $id})
            SET fn += {
                name:         $name,
                fundType:     $fundType,
                vintage:      $vintage,
                aumMillions:  $aum,
                manager:      $manager,
                status:       $status,
                sourceSystem: 'MANUAL',
                createdAt:    $ts,
                updatedAt:    $ts
            }
        """, id=f["id"], name=f["name"], fundType=f["fundType"],
             vintage=f["vintage"], aum=f["aumMillions"],
             manager=f["manager"], status=f["status"], ts=now_str())
    print(f"   {len(FUNDS)} funds seeded.")

    # ── Deals ────────────────────────────────────────────────────────────────
    print("→ Seeding deals...")
    for d in DEALS:
        tx.run("""
            MERGE (dl:Deal {id: $id})
            SET dl += {
                dealName:         $dealName,
                dealType:         $dealType,
                dealSizeMillions: $size,
                currency:         $currency,
                status:           $status,
                closeDate:        $closeDate,
                sourceSystem:     'MANUAL',
                createdAt:        $ts,
                updatedAt:        $ts
            }
        """, id=d["id"], dealName=d["dealName"], dealType=d["dealType"],
             size=d["dealSizeMillions"], currency=d["currency"],
             status=d["status"], closeDate=d.get("closeDate"), ts=now_str())
    print(f"   {len(DEALS)} deals seeded.")

    # ── Instruments ──────────────────────────────────────────────────────────
    print("→ Seeding instruments...")
    for i in INSTRUMENTS:
        tx.run("""
            MERGE (ins:Instrument {id: $id})
            SET ins += {
                name:              $name,
                instrumentType:    $instrumentType,
                isin:              $isin,
                faceValueMillions: $faceValue,
                coupon:            $coupon,
                maturityDate:      $maturityDate,
                currency:          $currency,
                sourceSystem:      'MANUAL',
                createdAt:         $ts,
                updatedAt:         $ts
            }
        """, id=i["id"], name=i["name"], instrumentType=i["instrumentType"],
             isin=i.get("isin"), faceValue=i["faceValueMillions"],
             coupon=i.get("coupon"), maturityDate=i.get("maturityDate"),
             currency=i["currency"], ts=now_str())
    print(f"   {len(INSTRUMENTS)} instruments seeded.")

    # ── Risks ────────────────────────────────────────────────────────────────
    print("→ Seeding risks...")
    for r in RISKS:
        tx.run("""
            MERGE (rk:Risk {id: $id})
            SET rk += {
                riskType:    $riskType,
                severity:    $severity,
                description: $description,
                flaggedDate: $flaggedDate,
                isActive:    $isActive,
                flaggedBy:   $flaggedBy,
                sourceSystem:'MANUAL',
                createdAt:   $ts,
                updatedAt:   $ts
            }
        """, id=r["id"], riskType=r["riskType"], severity=r["severity"],
             description=r["description"], flaggedDate=r["flaggedDate"],
             isActive=r["isActive"], flaggedBy=r["flaggedBy"], ts=now_str())
    print(f"   {len(RISKS)} risks seeded.")

    # ── Investors ────────────────────────────────────────────────────────────
    print("→ Seeding investors...")
    for inv in INVESTORS:
        tx.run("""
            MERGE (iv:Investor {id: $id})
            SET iv += {
                name:                    $name,
                investorType:            $investorType,
                domicile:                $domicile,
                committedCapitalMillions:$committed,
                sourceSystem:            'MANUAL',
                createdAt:               $ts,
                updatedAt:               $ts
            }
        """, id=inv["id"], name=inv["name"], investorType=inv["investorType"],
             domicile=inv["domicile"], committed=inv["committedCapitalMillions"], ts=now_str())
    print(f"   {len(INVESTORS)} investors seeded.")

    # ── Persons ──────────────────────────────────────────────────────────────
    print("→ Seeding persons...")
    for p in PERSONS:
        tx.run("""
            MERGE (pp:Person {id: $id})
            SET pp += {
                fullName:    $fullName,
                role:        $role,
                email:       $email,
                sourceSystem:'MANUAL',
                createdAt:   $ts,
                updatedAt:   $ts
            }
        """, id=p["id"], fullName=p["fullName"], role=p["role"],
             email=p.get("email"), ts=now_str())
    print(f"   {len(PERSONS)} persons seeded.")


def seed_relationships(tx):
    ed = AS_OF_DATE  # effective date for all initial relationships

    # ─────────────────────────────────────────────────────────────────────────
    # CHAIN A: 4-hop fund → risk path
    #   Apex Senior Credit Fund III
    #     → [HAS_POSITION] → TechNova TLA
    #       → [ISSUED_BY]  → TechNova Solutions
    #         → [SUBSIDIARY_OF] → Continental Group
    #           → [SUBSIDIARY_OF] → GlobalHoldings Corp
    #             ← [ATTACHED_TO] ← REGULATORY_WATCH risk
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Building Chain A (4-hop: Apex Credit → TechNova TLA → GlobalHoldings REGULATORY_WATCH)...")
    _has_position(tx, "f-apex-scf3", "ins-tna-tla", weightPct=9.52, valueMM=200.0)
    _issued_by(tx, "ins-tna-tla", "co-technova")
    _subsidiary_of(tx, "co-technova", "co-continental", ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-continental", "co-global",   ownershipPct=78.5,  ownershipType="DIRECT", ed=ed)
    _attached_to(tx, "risk-rw-global", "co-global", reason="REGULATORY_WATCH on UBO")

    # Also TLB position
    _has_position(tx, "f-apex-scf3", "ins-tna-tlb", weightPct=9.52, valueMM=200.0)
    _issued_by(tx, "ins-tna-tlb", "co-technova")

    # ─────────────────────────────────────────────────────────────────────────
    # CHAIN B: 3-hop fund → risk path
    #   Meridian Direct Lending IV
    #     → [HAS_POSITION] → MedCore Senior Note
    #       → [ISSUED_BY]  → MedCore Devices Corp
    #         → [SUBSIDIARY_OF] → MedCore Europe BV
    #           → [SUBSIDIARY_OF] → EuroGroup Holdco
    #             ← [ATTACHED_TO] ← REGULATORY_WATCH risk
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Building Chain B (3-hop: Meridian → MedCore → EuroGroup REGULATORY_WATCH)...")
    _has_position(tx, "f-merid-dl4", "ins-mdc-snr", weightPct=4.41, valueMM=150.0)
    _issued_by(tx, "ins-mdc-snr", "co-medcore")
    _subsidiary_of(tx, "co-medcore",    "co-medcore-eu", ownershipPct=100.0, ownershipType="DIRECT",   ed=ed)
    _subsidiary_of(tx, "co-medcore-eu", "co-euro",       ownershipPct=62.0,  ownershipType="DIRECT",   ed=ed)
    _attached_to(tx, "risk-rw-euro", "co-euro", reason="REGULATORY_WATCH on intermediate holdco")
    _attached_to(tx, "risk-cr-swiftlog", "co-swiftlog", reason="Covenant breach flagged")

    # ─────────────────────────────────────────────────────────────────────────
    # CHAIN C: 5-hop fund → risk path
    #   Keystone Opportunistic Credit II
    #     → [HAS_POSITION] → SwiftLogistics TLB
    #       → [ISSUED_BY]  → SwiftLogistics Group
    #         → [SUBSIDIARY_OF] → SwiftLogistics Europe SARL
    #           → [SUBSIDIARY_OF] → EuroGroup Holdco
    #             → [SUBSIDIARY_OF] → GlobalHoldings Corp
    #               ← [ATTACHED_TO] ← REGULATORY_WATCH risk
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Building Chain C (5-hop: Keystone → SwiftLogistics → GlobalHoldings REGULATORY_WATCH)...")
    _has_position(tx, "f-key-opp2", "ins-sl-tlb", weightPct=15.56, valueMM=280.0)
    _issued_by(tx, "ins-sl-tlb", "co-swiftlog")
    _subsidiary_of(tx, "co-swiftlog",    "co-swiftlog-eu", ownershipPct=100.0, ownershipType="DIRECT",   ed=ed)
    _subsidiary_of(tx, "co-swiftlog-eu", "co-euro",        ownershipPct=55.0,  ownershipType="DIRECT",   ed=ed)
    # EuroGroup is already linked to GlobalHoldings via Chain B's intermediate, add direct link
    _subsidiary_of(tx, "co-euro",        "co-global",      ownershipPct=45.0,  ownershipType="INDIRECT", ed=ed)

    # ─────────────────────────────────────────────────────────────────────────
    # Additional ISSUED_BY relationships
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Seeding remaining ISSUED_BY relationships...")
    _issued_by(tx, "ins-tna-rcf",  "co-technova")
    _issued_by(tx, "ins-mdc-tla",  "co-medcore")
    _issued_by(tx, "ins-ret-tla",  "co-retailedge")
    _issued_by(tx, "ins-ret-tlb",  "co-retailedge")
    _issued_by(tx, "ins-ret-snr",  "co-retailedge")
    _issued_by(tx, "ins-pe-tla",   "co-primeenergy")
    _issued_by(tx, "ins-pe-mezz",  "co-primeenergy")
    _issued_by(tx, "ins-sl-tla",   "co-swiftlog")
    _issued_by(tx, "ins-cb-pref",  "co-cloudbase")
    _issued_by(tx, "ins-pmfg-tla", "co-precmfg")
    _issued_by(tx, "ins-pmfg-rcf", "co-precmfg")
    _issued_by(tx, "ins-hp-tla",   "co-healthplus")
    _issued_by(tx, "ins-hp-tlb",   "co-healthplus")
    _issued_by(tx, "ins-hp-rcf",   "co-healthplus")
    _issued_by(tx, "ins-ms-snr",   "co-mediastream")
    _issued_by(tx, "ins-fp-pref",  "co-finpayments")

    # ─────────────────────────────────────────────────────────────────────────
    # Additional ownership chains (non-primary)
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Seeding additional subsidiary chains...")
    _subsidiary_of(tx, "co-retailedge",  "co-retail-us",   ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-retail-us",   "co-continental", ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-primeenergy", "co-energy-opco", ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-energy-opco", "co-global",      ownershipPct=60.0,  ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-cloudbase",   "co-cloud-eu",    ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-cloud-eu",    "co-continental", ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-healthplus",  "co-health-eu",   ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-mediastream", "co-media-us",    ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-media-us",    "co-finservgrp",  ownershipPct=55.0,  ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-finpayments", "co-fin-apac",    ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-fin-apac",    "co-pacific",     ownershipPct=70.0,  ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-precmfg",     "co-precmfg-asia",ownershipPct=100.0, ownershipType="DIRECT", ed=ed)
    _subsidiary_of(tx, "co-precmfg-asia","co-finservgrp",  ownershipPct=80.0,  ownershipType="DIRECT", ed=ed)

    # ─────────────────────────────────────────────────────────────────────────
    # HAS_POSITION — remaining fund positions
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Seeding fund positions...")
    # Apex Senior Credit Fund III
    _has_position(tx, "f-apex-scf3", "ins-ret-tla",   weightPct=5.71,  valueMM=120.0)
    _has_position(tx, "f-apex-scf3", "ins-pe-tla",    weightPct=4.29,  valueMM= 90.0)
    _has_position(tx, "f-apex-scf3", "ins-hp-tla",    weightPct=8.33,  valueMM=175.0)
    _has_position(tx, "f-apex-scf3", "ins-ms-snr",    weightPct=3.81,  valueMM= 80.0)

    # Meridian Direct Lending IV
    _has_position(tx, "f-merid-dl4", "ins-mdc-tla",   weightPct=2.94,  valueMM=100.0)
    _has_position(tx, "f-merid-dl4", "ins-sl-tla",    weightPct=5.29,  valueMM=180.0)
    _has_position(tx, "f-merid-dl4", "ins-hp-tlb",    weightPct=5.29,  valueMM=180.0)
    _has_position(tx, "f-merid-dl4", "ins-ret-tlb",   weightPct=4.41,  valueMM=150.0)
    _has_position(tx, "f-merid-dl4", "ins-pe-mezz",   weightPct=1.18,  valueMM= 40.0)

    # Keystone Opportunistic Credit II
    _has_position(tx, "f-key-opp2",  "ins-sl-tla",    weightPct=10.00, valueMM=180.0)
    _has_position(tx, "f-key-opp2",  "ins-pe-mezz",   weightPct=2.22,  valueMM= 40.0)
    _has_position(tx, "f-key-opp2",  "ins-pmfg-tla",  weightPct=6.67,  valueMM=120.0)

    # Atlas Capital Partners VI
    _has_position(tx, "f-atlas-cp6", "ins-cb-pref",   weightPct=1.79,  valueMM= 75.0)
    _has_position(tx, "f-atlas-cp6", "ins-fp-pref",   weightPct=1.43,  valueMM= 60.0)
    _has_position(tx, "f-atlas-cp6", "ins-tna-tla",   weightPct=4.76,  valueMM=200.0)

    # Crown Strategic Partners
    _has_position(tx, "f-crown-sp",  "ins-ms-snr",    weightPct=2.86,  valueMM= 80.0)
    _has_position(tx, "f-crown-sp",  "ins-ret-snr",   weightPct=3.57,  valueMM=100.0)
    _has_position(tx, "f-crown-sp",  "ins-sl-tlb",    weightPct=10.00, valueMM=280.0)

    # Apex Distressed Debt Fund I
    _has_position(tx, "f-apex-dd1",  "ins-sl-tlb",    weightPct=23.33, valueMM=280.0)
    _has_position(tx, "f-apex-dd1",  "ins-ret-tla",   weightPct=10.00, valueMM=120.0)

    # Keystone Infrastructure Debt
    _has_position(tx, "f-key-infra", "ins-pe-tla",    weightPct=6.00,  valueMM= 90.0)
    _has_position(tx, "f-key-infra", "ins-pmfg-rcf",  weightPct=2.67,  valueMM= 40.0)
    _has_position(tx, "f-key-infra", "ins-hp-rcf",    weightPct=2.33,  valueMM= 35.0)

    # ─────────────────────────────────────────────────────────────────────────
    # DEAL relationships
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Seeding deal relationships...")
    _involves(tx, "d-technova-lbo",   "co-technova",  role="BORROWER")
    _involves(tx, "d-technova-lbo",   "co-apex",      role="SPONSOR")
    _involves(tx, "d-medcore-cf",     "co-medcore",   role="BORROWER")
    _involves(tx, "d-medcore-cf",     "co-meridian",  role="SPONSOR")
    _involves(tx, "d-swiftlog-lbo",   "co-swiftlog",  role="BORROWER")
    _involves(tx, "d-swiftlog-lbo",   "co-keystone",  role="SPONSOR")
    _involves(tx, "d-retailedge-lbo", "co-retailedge",role="BORROWER")
    _involves(tx, "d-retailedge-lbo", "co-apex",      role="SPONSOR")
    _involves(tx, "d-healthplus-lbo", "co-healthplus", role="BORROWER")
    _involves(tx, "d-healthplus-lbo", "co-atlas",     role="SPONSOR")
    _involves(tx, "d-cloudbase-ge",   "co-cloudbase", role="BORROWER")
    _involves(tx, "d-cloudbase-ge",   "co-atlas",     role="SPONSOR")
    _involves(tx, "d-primeenergy-cf", "co-primeenergy",role="BORROWER")
    _involves(tx, "d-finpay-ge",      "co-finpayments",role="BORROWER")

    _financed_by(tx, "d-technova-lbo",  "ins-tna-tla",  200.0)
    _financed_by(tx, "d-technova-lbo",  "ins-tna-tlb",  200.0)
    _financed_by(tx, "d-medcore-cf",    "ins-mdc-snr",  150.0)
    _financed_by(tx, "d-medcore-cf",    "ins-mdc-tla",  100.0)
    _financed_by(tx, "d-swiftlog-lbo",  "ins-sl-tla",   180.0)
    _financed_by(tx, "d-swiftlog-lbo",  "ins-sl-tlb",   280.0)
    _financed_by(tx, "d-retailedge-lbo","ins-ret-tla",  120.0)
    _financed_by(tx, "d-retailedge-lbo","ins-ret-tlb",  150.0)
    _financed_by(tx, "d-healthplus-lbo","ins-hp-tla",   175.0)
    _financed_by(tx, "d-healthplus-lbo","ins-hp-tlb",   180.0)

    _invested_in(tx, "f-apex-scf3",  "d-technova-lbo",   committed=400.0, called=380.0)
    _invested_in(tx, "f-atlas-cp6",  "d-technova-lbo",   committed= 50.0, called= 48.0)
    _invested_in(tx, "f-merid-dl4",  "d-medcore-cf",     committed=250.0, called=250.0)
    _invested_in(tx, "f-key-opp2",   "d-swiftlog-lbo",   committed=460.0, called=460.0)
    _invested_in(tx, "f-crown-sp",   "d-swiftlog-lbo",   committed= 90.0, called= 90.0)
    _invested_in(tx, "f-atlas-cp6",  "d-healthplus-lbo", committed=355.0, called=355.0)
    _invested_in(tx, "f-atlas-cp6",  "d-cloudbase-ge",   committed=125.0, called=125.0)

    # ─────────────────────────────────────────────────────────────────────────
    # Risk ATTACHED_TO (non-chain-anchor risks)
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Seeding remaining risk attachments...")
    _attached_to(tx, "risk-rw-finserv",  "co-finservgrp", reason="FCA conduct investigation")
    _attached_to(tx, "risk-cr-retailedge","co-retailedge", reason="Declining DSC coverage")
    _attached_to(tx, "risk-cr-precmfg",  "co-precmfg",    reason="Margin compression")
    _attached_to(tx, "risk-op-swlbo",    "d-swiftlog-lbo", reason="Integration delays")
    _attached_to(tx, "risk-op-hplbo",    "d-healthplus-lbo","reason=Management transition")
    _attached_to(tx, "risk-mkt-eurobond","ins-mdc-snr",   reason="FX rate risk")
    _attached_to(tx, "risk-mkt-floater", "ins-sl-tlb",    reason="Rate environment risk")

    # ─────────────────────────────────────────────────────────────────────────
    # LP_IN investor relationships
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Seeding investor LP relationships...")
    _lp_in(tx, "inv-pension-us",  "f-apex-scf3",  500.0, "ANCHOR")
    _lp_in(tx, "inv-swf-gulf",    "f-apex-scf3",  400.0, "ANCHOR")
    _lp_in(tx, "inv-endow-ivy",   "f-apex-scf3",  250.0, "STANDARD")
    _lp_in(tx, "inv-pension-eu",  "f-merid-dl4",  600.0, "ANCHOR")
    _lp_in(tx, "inv-ins-eu",      "f-merid-dl4",  300.0, "STANDARD")
    _lp_in(tx, "inv-ins-uk",      "f-merid-dl4",  350.0, "STANDARD")
    _lp_in(tx, "inv-swf-asia",    "f-key-opp2",   400.0, "ANCHOR")
    _lp_in(tx, "inv-fof-uk",      "f-key-opp2",   200.0, "STANDARD")
    _lp_in(tx, "inv-swf-gulf",    "f-atlas-cp6",  800.0, "ANCHOR")
    _lp_in(tx, "inv-corp-us",     "f-atlas-cp6",  100.0, "CO_INVEST")
    _lp_in(tx, "inv-family-us",   "f-crown-sp",   150.0, "CO_INVEST")

    # ─────────────────────────────────────────────────────────────────────────
    # SERVES_ON_BOARD_OF
    # ─────────────────────────────────────────────────────────────────────────
    print("→ Seeding board relationships...")
    _serves_on(tx, "p-ceo-technova", "co-technova", "EXECUTIVE")
    _serves_on(tx, "p-rep-apex",     "co-technova", "SPONSOR_REP")
    _serves_on(tx, "p-cfo-swiftlog", "co-swiftlog", "EXECUTIVE")
    _serves_on(tx, "p-ceo-global",   "co-global",   "EXECUTIVE")
    _serves_on(tx, "p-cfo-medcore",  "co-medcore",  "EXECUTIVE")

    print("✓ All relationships seeded.")


# ─────────────────────────────────────────────────────────────────────────────
# Relationship helpers
# ─────────────────────────────────────────────────────────────────────────────

def _has_position(tx, fund_id, ins_id, weightPct, valueMM):
    tx.run("""
        MATCH (f:Fund {id: $fid}), (i:Instrument {id: $iid})
        MERGE (f)-[r:HAS_POSITION {asOfDate: $asOf}]->(i)
        SET r.weightPct             = $weight,
            r.currentValueMillions  = $value,
            r.effectiveDate         = $asOf,
            r.sourceSystem          = 'MANUAL'
    """, fid=fund_id, iid=ins_id, asOf=AS_OF_DATE, weight=weightPct, value=valueMM)

def _issued_by(tx, ins_id, company_id):
    tx.run("""
        MATCH (i:Instrument {id: $iid}), (c:Company {id: $cid})
        MERGE (i)-[:ISSUED_BY {effectiveDate: $ed, sourceSystem: 'MANUAL'}]->(c)
    """, iid=ins_id, cid=company_id, ed=AS_OF_DATE)

def _subsidiary_of(tx, child_id, parent_id, ownershipPct, ownershipType, ed):
    tx.run("""
        MATCH (child:Company {id: $cid}), (parent:Company {id: $pid})
        MERGE (child)-[r:SUBSIDIARY_OF {effectiveDate: $ed}]->(parent)
        SET r.ownershipPct  = $pct,
            r.ownershipType = $ot,
            r.expiryDate    = null,
            r.sourceSystem  = 'MANUAL'
    """, cid=child_id, pid=parent_id, pct=ownershipPct, ot=ownershipType, ed=ed)

def _attached_to(tx, risk_id, entity_id, reason):
    tx.run("""
        MATCH (r:Risk {id: $rid}), (e {id: $eid})
        MERGE (r)-[:ATTACHED_TO {effectiveDate: $ed, sourceSystem: 'MANUAL',
                                  attachmentReason: $reason}]->(e)
    """, rid=risk_id, eid=entity_id, ed=AS_OF_DATE, reason=reason)

def _involves(tx, deal_id, company_id, role):
    tx.run("""
        MATCH (d:Deal {id: $did}), (c:Company {id: $cid})
        MERGE (d)-[:INVOLVES {role: $role, effectiveDate: $ed, sourceSystem: 'MANUAL'}]->(c)
    """, did=deal_id, cid=company_id, role=role, ed=AS_OF_DATE)

def _financed_by(tx, deal_id, ins_id, allocated):
    tx.run("""
        MATCH (d:Deal {id: $did}), (i:Instrument {id: $iid})
        MERGE (d)-[:FINANCED_BY {allocatedMillions: $alloc, effectiveDate: $ed, sourceSystem: 'MANUAL'}]->(i)
    """, did=deal_id, iid=ins_id, alloc=allocated, ed=AS_OF_DATE)

def _invested_in(tx, fund_id, deal_id, committed, called):
    tx.run("""
        MATCH (f:Fund {id: $fid}), (d:Deal {id: $did})
        MERGE (f)-[:INVESTED_IN {effectiveDate: $ed, sourceSystem: 'MANUAL'}]->(d)
        SET r.committedMillions = $committed, r.calledMillions = $called
    """, fid=fund_id, did=deal_id, committed=committed, called=called, ed=AS_OF_DATE)

def _lp_in(tx, investor_id, fund_id, commitment, commitment_type):
    tx.run("""
        MATCH (iv:Investor {id: $ivid}), (f:Fund {id: $fid})
        MERGE (iv)-[:LP_IN {commitmentMillions: $c, commitmentType: $ct,
                             effectiveDate: $ed, sourceSystem: 'MANUAL'}]->(f)
    """, ivid=investor_id, fid=fund_id, c=commitment, ct=commitment_type, ed=AS_OF_DATE)

def _serves_on(tx, person_id, company_id, board_role):
    tx.run("""
        MATCH (p:Person {id: $pid}), (c:Company {id: $cid})
        MERGE (p)-[:SERVES_ON_BOARD_OF {boardRole: $role, effectiveDate: $ed, sourceSystem: 'MANUAL'}]->(c)
    """, pid=person_id, cid=company_id, role=board_role, ed=AS_OF_DATE)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ECG Graph Seed Script")
    parser.add_argument("--wipe", action="store_true", help="Clear all data before seeding")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("ECG Platform — Graph Seed Script")
    print(f"Target: {NEO4J_URI}")
    print(f"{'='*60}\n")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

    try:
        with driver.session() as session:
            print("Phase 1: Seeding nodes...")
            session.execute_write(seed, wipe=args.wipe)

            print("\nPhase 2: Seeding relationships...")
            session.execute_write(seed_relationships)

        print("\n" + "="*60)
        print("✓  Graph seeding complete!")
        print(f"   Companies:   {len(ALL_COMPANIES)}")
        print(f"   Funds:       {len(FUNDS)}")
        print(f"   Deals:       {len(DEALS)}")
        print(f"   Instruments: {len(INSTRUMENTS)}")
        print(f"   Risks:       {len(RISKS)}")
        print(f"   Investors:   {len(INVESTORS)}")
        print(f"   Persons:     {len(PERSONS)}")
        print("\nVerify in Neo4j Browser (http://localhost:7474):")
        print("  MATCH (f:Fund {name:'Apex Senior Credit Fund III'})")
        print("  -[:HAS_POSITION]->(i:Instrument)-[:ISSUED_BY]->(c:Company)")
        print("  -[:SUBSIDIARY_OF*1..4]->(p:Company)<-[:ATTACHED_TO]-(r:Risk)")
        print("  RETURN f,i,c,p,r")
        print("="*60 + "\n")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
