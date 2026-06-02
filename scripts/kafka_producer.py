"""
ECG Platform — Kafka Event Producer
Simulates a stream of 50 real-time capital markets events:
  - 20 deal created events
  - 15 ownership change events (including new multi-hop paths)
  - 15 risk flagged events

PARTITION KEY STRATEGY:
  Each event type is keyed by its canonical entity ID, NOT the event ID.
  This ensures ordering per entity across all partitions:
    - Deal events     → keyed by DealId
    - Ownership events→ keyed by ChildCompanyId
    - Risk events     → keyed by AttachedToEntityId

Usage:
  pip install confluent-kafka
  python scripts/kafka_producer.py
  python scripts/kafka_producer.py --count 100 --delay-ms 200
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timedelta
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"

DEAL_TOPIC      = "ecg.deal.created"
STATUS_TOPIC    = "ecg.deal.status.changed"
OWNERSHIP_TOPIC = "ecg.ownership.changed"
RISK_TOPIC      = "ecg.risk.flagged"
VALUATION_TOPIC = "ecg.valuation.updated"

COMPANY_IDS = [
    "co-technova", "co-medcore", "co-retailedge", "co-primeenergy",
    "co-swiftlog", "co-cloudbase", "co-precmfg", "co-healthplus",
    "co-mediastream", "co-finpayments", "co-global", "co-euro",
    "co-pacific", "co-continental", "co-finservgrp",
    "co-technova-us", "co-medcore-eu", "co-swiftlog-eu",
]

FUND_IDS = [
    "f-apex-scf3", "f-merid-dl4", "f-key-opp2",
    "f-atlas-cp6", "f-crown-sp", "f-apex-dd1",
    "f-merid-rec", "f-key-infra",
]

INSTRUMENT_IDS = [
    "ins-tna-tla", "ins-tna-tlb", "ins-mdc-snr",
    "ins-sl-tla", "ins-sl-tlb", "ins-hp-tla",
    "ins-ret-tla", "ins-ret-tlb", "ins-pe-tla",
]


def produce(producer: Producer, topic: str, key: str, payload: dict):
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(payload, default=str),
        callback=delivery_callback
    )
    producer.poll(0)


def delivery_callback(err, msg):
    if err:
        print(f"   ✗  Delivery failed: {err}")
    else:
        print(f"   ✓  [{msg.topic()}] partition={msg.partition()} offset={msg.offset()} key={msg.key().decode()}")


# ─────────────────────────────────────────────────────────────────────────────
# Event generators
# ─────────────────────────────────────────────────────────────────────────────

def gen_deal_created(index: int) -> tuple[str, str, dict]:
    sectors    = ["Technology", "Healthcare", "Industrials", "Energy", "Consumer"]
    deal_types = ["LBO", "CREDIT_FACILITY", "GROWTH_EQUITY", "MEZZ", "RECAPITALIZATION"]
    currencies = ["USD", "EUR", "GBP"]

    deal_id     = f"deal-stream-{index:04d}-{uuid.uuid4().hex[:6]}"
    borrower_id = COMPANY_IDS[index % len(COMPANY_IDS)]
    sponsor_id  = COMPANY_IDS[(index + 3) % len(COMPANY_IDS)]
    fund_ids    = [FUND_IDS[index % len(FUND_IDS)], FUND_IDS[(index+1) % len(FUND_IDS)]]

    payload = {
        "eventId":         f"evt-{uuid.uuid4().hex}",
        "schemaVersion":   "1.0",
        "timestamp":       datetime.utcnow().isoformat(),
        "dealId":          deal_id,
        "dealName":        f"StreamDeal-{index:04d} {deal_types[index % len(deal_types)]}",
        "dealType":        deal_types[index % len(deal_types)],
        "dealSizeMillions":round(50 + (index * 23.7) % 450, 1),
        "currency":        currencies[index % len(currencies)],
        "borrowerCompanyId": borrower_id,
        "sponsorCompanyId":  sponsor_id,
        "fundIds":           fund_ids,
        "status":           "PIPELINE",
        "closeDate":        None,
        "sourceSystem":     "DEAL_MGMT_SYSTEM"
    }
    return DEAL_TOPIC, deal_id, payload


def gen_ownership_changed(index: int) -> tuple[str, str, dict]:
    """
    Simulates a corporate restructuring event.
    Every 5th event creates a NEW multi-hop path by moving a subsidiary
    under GlobalHoldings Corp, extending exposure chains.
    """
    child_id    = COMPANY_IDS[index % len(COMPANY_IDS)]
    old_parent  = COMPANY_IDS[(index + 2) % len(COMPANY_IDS)]
    new_parent  = COMPANY_IDS[(index + 4) % len(COMPANY_IDS)]
    reasons     = ["ACQUISITION", "RESTRUCTURING", "DIVESTITURE", "REORGANIZATION"]

    # Every 5th: route a company under GlobalHoldings Corp to extend chain A
    if index % 5 == 0:
        new_parent = "co-global"

    payload = {
        "eventId":          f"evt-{uuid.uuid4().hex}",
        "schemaVersion":    "1.0",
        "timestamp":        datetime.utcnow().isoformat(),
        "childCompanyId":   child_id,
        "oldParentCompanyId": old_parent if old_parent != new_parent else None,
        "newParentCompanyId": new_parent,
        "newOwnershipPct":  round(51.0 + (index * 7.3) % 49.0, 1),
        "ownershipType":    "DIRECT",
        "changeReason":     reasons[index % len(reasons)],
        "effectiveDate":    (datetime.utcnow() - timedelta(days=index % 30)).strftime("%Y-%m-%d")
    }
    return OWNERSHIP_TOPIC, child_id, payload


def gen_risk_flagged(index: int) -> tuple[str, str, dict]:
    risk_types  = ["CREDIT", "REGULATORY_WATCH", "OPERATIONAL", "MARKET", "LIQUIDITY"]
    severities  = ["HIGH", "MEDIUM", "MEDIUM", "LOW", "LOW"]  # weighted toward MEDIUM/LOW
    entities    = COMPANY_IDS + [f"ins-tna-tla", f"ins-sl-tlb", f"d-swiftlog-lbo"]
    flaggers    = ["SYSTEM", "ANALYST", "REGULATORY_FEED", "MARKET_DATA"]

    risk_type   = risk_types[index % len(risk_types)]
    severity    = severities[index % len(severities)]
    entity_id   = entities[index % len(entities)]
    risk_id     = f"risk-stream-{index:04d}-{uuid.uuid4().hex[:6]}"

    descriptions = {
        "CREDIT":           f"Credit deterioration detected on entity {entity_id}. Watch for covenant approach.",
        "REGULATORY_WATCH": f"Regulatory inquiry initiated on {entity_id}. Enhanced monitoring required.",
        "OPERATIONAL":      f"Key management departure at {entity_id}. Succession plan review requested.",
        "MARKET":           f"FX/rate sensitivity elevated for {entity_id}. Hedging review recommended.",
        "LIQUIDITY":        f"Liquidity cushion thin at {entity_id}. Monitor revolver availability.",
    }

    payload = {
        "eventId":              f"evt-{uuid.uuid4().hex}",
        "schemaVersion":        "1.0",
        "timestamp":            datetime.utcnow().isoformat(),
        "riskId":               risk_id,
        "riskType":             risk_type,
        "severity":             severity,
        "attachedToEntityId":   entity_id,
        "attachedToEntityType": "Company" if entity_id.startswith("co-") else "Instrument",
        "description":          descriptions[risk_type],
        "flaggedBy":            flaggers[index % len(flaggers)]
    }
    return RISK_TOPIC, entity_id, payload


def gen_deal_status_changed(index: int) -> tuple[str, str, dict]:
    deal_id  = f"deal-stream-{index:04d}-{uuid.uuid4().hex[:6]}"
    statuses = [("PIPELINE", "CLOSED"), ("CLOSED", "EXITED"), ("PIPELINE", "ON_HOLD")]
    old_s, new_s = statuses[index % len(statuses)]

    payload = {
        "eventId":       f"evt-{uuid.uuid4().hex}",
        "schemaVersion": "1.0",
        "timestamp":     datetime.utcnow().isoformat(),
        "dealId":        deal_id,
        "oldStatus":     old_s,
        "newStatus":     new_s,
        "effectiveDate": datetime.utcnow().strftime("%Y-%m-%d"),
        "changeNote":    f"Status change #{index}"
    }
    return STATUS_TOPIC, deal_id, payload


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ECG Kafka Event Producer")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--delay-ms", type=int, default=500, help="Delay between events (ms)")
    args = parser.parse_args()

    producer_conf = {
        "bootstrap.servers": args.bootstrap,
        "client.id":         "ecg-seed-producer",
        "acks":              "all",
        "retries":           3,
        "compression.type":  "snappy",
    }

    producer = Producer(producer_conf)

    print(f"\n{'='*60}")
    print("ECG Platform — Kafka Event Producer")
    print(f"Bootstrap: {args.bootstrap}")
    print(f"Delay: {args.delay_ms}ms between events")
    print(f"{'='*60}\n")

    event_plan = []

    # 20 deal created events
    for i in range(20):
        event_plan.append(("DEAL_CREATED", i))

    # 15 ownership changed events (including new chain extensions every 5th)
    for i in range(15):
        event_plan.append(("OWNERSHIP_CHANGED", i))

    # 15 risk flagged events
    for i in range(15):
        event_plan.append(("RISK_FLAGGED", i))

    total = len(event_plan)
    print(f"Producing {total} events across all ECG topics...\n")

    for seq, (event_type, index) in enumerate(event_plan, 1):
        if event_type == "DEAL_CREATED":
            topic, key, payload = gen_deal_created(index)
            label = f"DealCreated    deal={payload['dealId']}"
        elif event_type == "OWNERSHIP_CHANGED":
            topic, key, payload = gen_ownership_changed(index)
            label = f"OwnershipChanged child={payload['childCompanyId']} → parent={payload['newParentCompanyId']}"
        else:
            topic, key, payload = gen_risk_flagged(index)
            label = f"RiskFlagged    type={payload['riskType']} sev={payload['severity']} on={payload['attachedToEntityId']}"

        print(f"[{seq:02d}/{total}] {label}")
        produce(producer, topic, key, payload)

        if args.delay_ms > 0:
            time.sleep(args.delay_ms / 1000.0)

    # Flush remaining messages
    producer.flush(timeout=30)
    print(f"\n✓  All {total} events produced.")
    print("   Watch the graph update at: http://localhost:7474")
    print("   Query: MATCH (r:Risk)-[:ATTACHED_TO]->(c:Company) RETURN r,c LIMIT 25\n")


if __name__ == "__main__":
    main()
