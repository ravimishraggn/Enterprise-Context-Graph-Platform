"""
ECG Platform — Mock Financial Data Warehouse API
Simulates a data warehouse endpoint returning synthetic financial metrics.
Used by the ECG Federation layer for data enrichment alongside Neo4j graph results.

Usage:
  pip install fastapi uvicorn
  uvicorn mock_warehouse_api:app --host 0.0.0.0 --port 8090 --reload
"""

import random
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="ECG Mock Financial Data Warehouse",
    description="Synthetic financial metrics for ECG federation enrichment",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# Deterministic "realistic" metrics per entity (seed from entity ID hash)
# ─────────────────────────────────────────────────────────────────────────────
COMPANY_PROFILES = {
    "co-technova":    {"revenue": 389.0, "ebitda_margin": 0.32, "leverage": 4.2,  "coverage": 3.8, "rating": "B+"},
    "co-medcore":     {"revenue": 185.0, "ebitda_margin": 0.335,"leverage": 3.9,  "coverage": 3.2, "rating": "B+"},
    "co-retailedge":  {"revenue": 640.0, "ebitda_margin": 0.14, "leverage": 5.8,  "coverage": 1.05,"rating": "B"},
    "co-primeenergy": {"revenue": 195.0, "ebitda_margin": 0.625,"leverage": 2.4,  "coverage": 5.1, "rating": "BB-"},
    "co-swiftlog":    {"revenue": 520.0, "ebitda_margin": 0.27, "leverage": 7.2,  "coverage": 1.8, "rating": "CCC+"},
    "co-cloudbase":   {"revenue":  89.0, "ebitda_margin": -0.05,"leverage": 0.0,  "coverage": 0.0, "rating": "N/A"},
    "co-precmfg":     {"revenue": 380.0, "ebitda_margin": 0.18, "leverage": 5.1,  "coverage": 2.2, "rating": "B"},
    "co-healthplus":  {"revenue": 420.0, "ebitda_margin": 0.20, "leverage": 4.5,  "coverage": 2.8, "rating": "B+"},
    "co-mediastream": {"revenue": 290.0, "ebitda_margin": 0.22, "leverage": 3.8,  "coverage": 3.5, "rating": "B+"},
    "co-finpayments": {"revenue":  45.0, "ebitda_margin": -0.12,"leverage": 0.0,  "coverage": 0.0, "rating": "N/A"},
    "co-global":      {"revenue":4200.0, "ebitda_margin": 0.15, "leverage": 3.2,  "coverage": 4.2, "rating": "BB"},
    "co-euro":        {"revenue":1800.0, "ebitda_margin": 0.18, "leverage": 2.9,  "coverage": 4.8, "rating": "BB"},
    "co-continental": {"revenue":2100.0, "ebitda_margin": 0.16, "leverage": 3.5,  "coverage": 3.9, "rating": "BB-"},
}

def get_profile(company_id: str) -> dict:
    if company_id in COMPANY_PROFILES:
        return COMPANY_PROFILES[company_id]
    # Generate deterministic synthetic profile for unknown IDs
    rng = random.Random(hash(company_id))
    return {
        "revenue":       round(50  + rng.random() * 500,  1),
        "ebitda_margin": round(0.08 + rng.random() * 0.27, 3),
        "leverage":      round(3.0  + rng.random() * 5.0,  2),
        "coverage":      round(1.5  + rng.random() * 3.5,  2),
        "rating":        rng.choice(["B-", "B", "B+", "BB-", "BB", "BB+"]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "healthy", "service": "ECG Mock Warehouse API", "timestamp": datetime.utcnow().isoformat()}


@app.get("/metrics/{company_id}")
def get_metrics(company_id: str):
    """
    Returns synthetic financial metrics for a company.
    In production, this would query a data warehouse or financial data vendor.
    """
    profile = get_profile(company_id)
    rng = random.Random(hash(company_id + datetime.utcnow().strftime("%Y%m%d")))

    # Add small daily noise to revenue/margins (simulate live data)
    noise = lambda x, pct=0.02: round(x * (1 + rng.uniform(-pct, pct)), 2)

    return {
        "company_id":             company_id,
        "revenue_ttm_millions":   noise(profile["revenue"]),
        "ebitda_ttm_millions":    noise(round(profile["revenue"] * profile["ebitda_margin"], 1)),
        "ebitda_margin":          noise(profile["ebitda_margin"], 0.01),
        "leverage_ratio":         noise(profile["leverage"], 0.05),
        "interest_coverage_ratio":noise(profile["coverage"], 0.05),
        "credit_rating":          profile["rating"],
        "net_debt_millions":      round(profile["revenue"] * profile["ebitda_margin"] * profile["leverage"], 1),
        "fcf_conversion_pct":     round(0.35 + rng.random() * 0.30, 2),
        "revenue_growth_yoy_pct": round(-0.05 + rng.random() * 0.35, 3),
        "currency":               "USD",
        "reporting_period":       "LTM",
        "as_of_date":             (datetime.utcnow() - timedelta(days=rng.randint(0, 30))).strftime("%Y-%m-%d"),
        "last_updated":           datetime.utcnow().isoformat(),
        "data_source":            "FINANCIAL_DATA_WAREHOUSE_v2",
    }


@app.get("/metrics/batch")
def get_metrics_batch(company_ids: str):
    """
    Batch metrics endpoint — comma-separated company IDs.
    Example: /metrics/batch?company_ids=co-technova,co-medcore
    """
    ids = [cid.strip() for cid in company_ids.split(",") if cid.strip()]
    if len(ids) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 IDs per batch request")
    return {"results": [get_metrics(cid) for cid in ids]}


@app.get("/fund-metrics/{fund_id}")
def get_fund_metrics(fund_id: str):
    """Returns synthetic NAV and performance metrics for a fund."""
    rng = random.Random(hash(fund_id))
    irr  = round(0.08 + rng.random() * 0.20, 4)
    moic = round(1.2  + rng.random() * 1.5,  2)
    nav  = round(100  + rng.random() * 3000,  1)

    return {
        "fund_id":                    fund_id,
        "nav_millions":               nav,
        "gross_irr":                  irr,
        "net_irr":                    round(irr * 0.82, 4),
        "moic":                       moic,
        "dpi":                        round(moic * rng.uniform(0.3, 0.8), 2),
        "tvpi":                       moic,
        "deployment_pct":             round(0.60 + rng.random() * 0.35, 2),
        "weighted_avg_leverage":      round(4.2 + rng.random() * 2.0,  2),
        "weighted_avg_coverage":      round(1.8 + rng.random() * 2.0,  2),
        "positions_in_watchlist":     rng.randint(0, 5),
        "weighted_avg_credit_rating": rng.choice(["B", "B+", "B/B+"]),
        "as_of_date":                 datetime.utcnow().strftime("%Y-%m-%d"),
        "last_updated":               datetime.utcnow().isoformat(),
    }


@app.get("/covenants/{company_id}")
def get_covenants(company_id: str):
    """Returns synthetic covenant headroom data."""
    profile = get_profile(company_id)
    rng     = random.Random(hash(company_id))
    breach  = profile["leverage"] > 6.5

    return {
        "company_id":         company_id,
        "leverage_covenant":  6.5,
        "leverage_actual":    profile["leverage"],
        "leverage_headroom":  round(6.5 - profile["leverage"], 2),
        "leverage_breach":    breach,
        "coverage_covenant":  2.0,
        "coverage_actual":    profile["coverage"],
        "coverage_headroom":  round(profile["coverage"] - 2.0, 2),
        "coverage_breach":    profile["coverage"] < 2.0,
        "last_test_date":     (datetime.utcnow() - timedelta(days=rng.randint(1, 90))).strftime("%Y-%m-%d"),
        "next_test_date":     (datetime.utcnow() + timedelta(days=rng.randint(30, 90))).strftime("%Y-%m-%d"),
    }
