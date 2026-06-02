// =============================================================================
// ECG Platform — Risk Propagation Query
//
// PURPOSE: Given a risk attached to a parent entity, find ALL downstream
//          funds and instruments that have indirect exposure through the
//          subsidiary ownership chain. This is the "blast radius" query.
//
// PARAMETERS:
//   $riskId   : STRING  — The ID of the Risk node to propagate from
//   $asOfDate : STRING  — ISO-8601 date for bi-temporal consistency
//
// DIRECTION: Risk propagates DOWN the ownership chain:
//   Risk → Parent Company ← SUBSIDIARY_OF ← Child Companies
//                         ← ISSUED_BY     ← Instruments
//                         ← HAS_POSITION  ← Funds
//
// This query answers: "A new REGULATORY_WATCH risk has been flagged on
//   GlobalHoldings Corp. Which funds should their portfolio managers be
//   briefed on TODAY?"
// =============================================================================

MATCH (r:Risk {id: $riskId})-[:ATTACHED_TO]->(riskOriginEntity)
// Fan out to all subsidiaries at any depth (including the entity itself, depth 0)
MATCH subsidPath = (riskOriginEntity)<-[:SUBSIDIARY_OF*0..5]-(affectedEntity:Company)
WHERE
    ALL(rel IN relationships(subsidPath)
        WHERE rel.effectiveDate <= $asOfDate
          AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate)
    )

// Walk from affected entities to their instruments and funds
OPTIONAL MATCH (instrument:Instrument)-[:ISSUED_BY]->(affectedEntity)
OPTIONAL MATCH (fund:Fund)-[pos:HAS_POSITION]->(instrument)
WHERE pos.asOfDate = $asOfDate

// Also pull any deals directly involving the affected company
OPTIONAL MATCH (deal:Deal)-[:INVOLVES]->(affectedEntity)
WHERE deal.status IN ['CLOSED', 'PIPELINE']

RETURN
    r.riskType                      AS riskType,
    r.severity                      AS severity,
    r.description                   AS riskDescription,
    r.flaggedDate                   AS flaggedDate,
    labels(riskOriginEntity)[0]     AS originEntityType,
    coalesce(riskOriginEntity.name, riskOriginEntity.dealName)
                                    AS riskOriginEntity,
    affectedEntity.name             AS affectedCompany,
    affectedEntity.sector           AS affectedSector,
    affectedEntity.jurisdiction     AS affectedJurisdiction,
    length(subsidPath)              AS ownershipDepth,
    instrument.isin                 AS instrumentIsin,
    instrument.instrumentType       AS instrumentType,
    instrument.faceValueMillions    AS instrumentFaceValue,
    pos.currentValueMillions        AS currentPositionValue,
    pos.weightPct                   AS portfolioWeightPct,
    fund.name                       AS affectedFund,
    fund.aumMillions                AS fundAum,
    deal.dealName                   AS relatedDeal,
    deal.dealType                   AS dealType,
    deal.status                     AS dealStatus

ORDER BY
    r.severity DESC,
    ownershipDepth ASC,
    pos.currentValueMillions DESC NULLS LAST;

// =============================================================================
// ── AGGREGATE VARIANT: Total dollar exposure per fund ──
// =============================================================================

// MATCH (r:Risk {id: $riskId})-[:ATTACHED_TO]->(origin)
// MATCH (origin)<-[:SUBSIDIARY_OF*0..5]-(company:Company)
// MATCH (instrument:Instrument)-[:ISSUED_BY]->(company)
// MATCH (fund:Fund)-[pos:HAS_POSITION]->(instrument)
// WHERE pos.asOfDate = $asOfDate
// WITH fund, r,
//      sum(pos.currentValueMillions)  AS totalExposureMillions,
//      count(DISTINCT instrument)     AS instrumentCount,
//      count(DISTINCT company)        AS affectedEntityCount
// RETURN
//     fund.name               AS fund,
//     r.severity              AS severity,
//     totalExposureMillions,
//     instrumentCount,
//     affectedEntityCount,
//     totalExposureMillions / fund.aumMillions * 100 AS pctOfNav
// ORDER BY totalExposureMillions DESC;
