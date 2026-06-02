// =============================================================================
// ECG Platform — Fund Full Portfolio View Query
//
// PURPOSE: Return the complete portfolio context for a fund — all positions
//          with their issuer, deal, and active risk overlay in one query.
//          This is the "fund intelligence" query used by portfolio managers.
//
// PARAMETERS:
//   $fundName : STRING — Exact name of the fund
//   $asOfDate : STRING — ISO-8601 portfolio snapshot date
//
// JOINS:
//   Fund → (via HAS_POSITION) → Instrument
//          (via ISSUED_BY)    → Company (issuer)
//          (via FINANCED_BY ← Deal)
//          (via ATTACHED_TO ← Risk, on company)
//
// NOTE: OPTIONAL MATCH is used for Deal and Risk so that positions without
//       a deal context or active risks are still returned.
// =============================================================================

MATCH (f:Fund {name: $fundName})-[pos:HAS_POSITION]->(i:Instrument)-[:ISSUED_BY]->(c:Company)
WHERE pos.asOfDate = $asOfDate

OPTIONAL MATCH (d:Deal)-[:FINANCED_BY]->(i)

OPTIONAL MATCH (r:Risk)-[:ATTACHED_TO]->(c)
WHERE r.isActive = true

// Collect risks into a list per instrument position (one row per position)
WITH f, pos, i, c, d,
     collect(DISTINCT {
         riskId:       r.id,
         riskType:     r.riskType,
         severity:     r.severity,
         description:  r.description,
         flaggedDate:  r.flaggedDate
     }) AS activeRisks

// Indirect risk: risks on PARENT companies (up to 3 hops)
OPTIONAL MATCH (parentRisk:Risk)-[:ATTACHED_TO]->(parent:Company)
              <-[:SUBSIDIARY_OF*1..3]-(c)
WHERE parentRisk.isActive = true

WITH f, pos, i, c, d, activeRisks,
     collect(DISTINCT {
         riskId:         parentRisk.id,
         riskType:       parentRisk.riskType,
         severity:       parentRisk.severity,
         description:    parentRisk.description,
         flaggedDate:    parentRisk.flaggedDate,
         attachedTo:     parent.name,
         hopsFromIssuer: 'INDIRECT'
     }) AS indirectRisks

RETURN
    f.name                                          AS fund,
    f.fundType                                      AS fundType,
    f.aumMillions                                   AS fundAumMillions,
    i.id                                            AS instrumentId,
    i.isin                                          AS isin,
    i.cusip                                         AS cusip,
    i.instrumentType                                AS instrumentType,
    i.faceValueMillions                             AS faceValueMillions,
    i.coupon                                        AS coupon,
    i.maturityDate                                  AS maturityDate,
    i.currency                                      AS currency,
    pos.currentValueMillions                        AS currentValueMillions,
    pos.weightPct                                   AS portfolioWeightPct,
    pos.asOfDate                                    AS positionAsOfDate,
    c.id                                            AS issuerId,
    c.name                                          AS issuer,
    c.sector                                        AS issuerSector,
    c.jurisdiction                                  AS issuerJurisdiction,
    c.companyType                                   AS issuerType,
    d.dealName                                      AS dealName,
    d.dealType                                      AS dealType,
    d.status                                        AS dealStatus,
    d.dealSizeMillions                              AS dealSizeMillions,
    d.closeDate                                     AS dealCloseDate,
    -- Direct risks on the issuer
    [risk IN activeRisks WHERE risk.riskId IS NOT NULL] AS directRisks,
    -- Risks propagated from parent entities
    [risk IN indirectRisks WHERE risk.riskId IS NOT NULL] AS indirectRisks,
    -- Convenience: highest severity across all risks (direct + indirect)
    CASE
        WHEN any(risk IN activeRisks WHERE risk.severity = 'HIGH')
          OR any(risk IN indirectRisks WHERE risk.severity = 'HIGH') THEN 'HIGH'
        WHEN any(risk IN activeRisks WHERE risk.severity = 'MEDIUM')
          OR any(risk IN indirectRisks WHERE risk.severity = 'MEDIUM') THEN 'MEDIUM'
        WHEN any(risk IN activeRisks WHERE risk.severity = 'LOW')
          OR any(risk IN indirectRisks WHERE risk.severity = 'LOW') THEN 'LOW'
        ELSE 'NONE'
    END AS worstRiskSeverity

ORDER BY pos.currentValueMillions DESC;

// =============================================================================
// ── SUMMARY STATS VARIANT: Portfolio sector concentration ──
// =============================================================================

// MATCH (f:Fund {name: $fundName})-[pos:HAS_POSITION]->(i:Instrument)-[:ISSUED_BY]->(c:Company)
// WHERE pos.asOfDate = $asOfDate
// RETURN
//     c.sector                        AS sector,
//     count(DISTINCT i)               AS positionCount,
//     sum(pos.currentValueMillions)   AS sectorExposureMillions,
//     sum(pos.weightPct)              AS sectorWeightPct
// ORDER BY sectorExposureMillions DESC;
