// =============================================================================
// ECG Platform — Multi-Hop Indirect Exposure Query
//
// PURPOSE: Find all funds that have indirect exposure to a given risk type
//          through a chain: Fund → Instrument → Issuer → Subsidiary Chain → Risk
//
// PARAMETERS:
//   $riskType   : STRING  — "REGULATORY_WATCH" | "CREDIT" | "OPERATIONAL" | "MARKET"
//   $asOfDate   : STRING  — ISO-8601 date for bi-temporal filtering (e.g. "2024-01-01")
//   $maxHops    : INTEGER — Maximum ownership chain depth (default 4)
//
// RETURNS: One row per (fund, risk) pair with the exposure path and hop count
//
// PERFORMANCE NOTE:
//   The SUBSIDIARY_OF*1..4 variable-length match is bounded. The relationship
//   property index on effectiveDate/expiryDate enables efficient temporal pruning.
//   GDS PageRank or Betweenness can identify systemically important nodes in the
//   ownership graph — see GDS layer for batch scoring.
// =============================================================================

MATCH path = (f:Fund)-[pos:HAS_POSITION]->(i:Instrument)
             -[:ISSUED_BY]->(directIssuer:Company)
             -[:SUBSIDIARY_OF*1..4]->(riskBearingEntity:Company)
             <-[:ATTACHED_TO]-(r:Risk)
WHERE
    -- Filter to requested risk type
    r.riskType = $riskType
    AND r.isActive = true
    AND r.severity IN ['HIGH', 'MEDIUM']

    -- Bi-temporal: only traverse active ownership relationships as of asOfDate
    AND ALL(rel IN relationships(path)
        WHERE type(rel) <> 'SUBSIDIARY_OF'
           OR (
               rel.effectiveDate <= $asOfDate
               AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate)
           )
    )

    -- Only active fund positions
    AND pos.asOfDate = $asOfDate

RETURN
    f.id                                                        AS fundId,
    f.name                                                      AS fund,
    f.aumMillions                                               AS fundAumMillions,
    f.fundType                                                  AS fundType,
    directIssuer.name                                           AS directIssuer,
    directIssuer.sector                                         AS issuerSector,
    riskBearingEntity.name                                      AS riskBearingEntity,
    riskBearingEntity.jurisdiction                              AS riskJurisdiction,
    length(path)                                                AS totalHops,
    -- Number of SUBSIDIARY_OF hops (structural depth of indirect exposure)
    length(path) - 2                                            AS ownershipHops,
    r.id                                                        AS riskId,
    r.riskType                                                  AS riskType,
    r.severity                                                  AS severity,
    r.description                                               AS riskDescription,
    r.flaggedDate                                               AS riskFlaggedDate,
    r.flaggedBy                                                 AS flaggedBy,
    pos.currentValueMillions                                    AS positionValueMillions,
    pos.weightPct                                               AS positionWeightPct,
    i.isin                                                      AS instrumentIsin,
    i.instrumentType                                            AS instrumentType,
    -- Full exposure path as ordered list of entity names
    [node IN nodes(path) | coalesce(node.name, node.dealName)] AS exposurePath,
    -- Ownership percentages along the chain (null for non-SUBSIDIARY_OF rels)
    [rel IN relationships(path)
        WHERE type(rel) = 'SUBSIDIARY_OF' | rel.ownershipPct]  AS ownershipChain

ORDER BY
    r.severity DESC,
    totalHops ASC,
    pos.currentValueMillions DESC;

// =============================================================================
// VARIANT: Aggregate exposure per fund across all matching risk paths
// Useful for portfolio-level risk dashboards
// =============================================================================

// MATCH path = (f:Fund)-[pos:HAS_POSITION]->(i:Instrument)
//              -[:ISSUED_BY]->(c:Company)
//              -[:SUBSIDIARY_OF*1..4]->(p:Company)
//              <-[:ATTACHED_TO]-(r:Risk)
// WHERE r.riskType = $riskType AND r.isActive = true
// WITH f, sum(pos.currentValueMillions) AS totalExposure, count(DISTINCT r) AS riskCount
// RETURN f.name AS fund, totalExposure, riskCount
// ORDER BY totalExposure DESC
