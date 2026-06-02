// =============================================================================
// ECG Platform — Ownership Chain Traversal Query
//
// PURPOSE: Trace the full corporate ownership chain from a given entity
//          up to its ultimate beneficial owner (UBO).
//          Supports bi-temporal querying: "what was the chain on date X?"
//
// PARAMETERS:
//   $companyName : STRING  — Name of the company to start traversal from
//   $asOfDate    : STRING  — ISO-8601 date for bi-temporal snapshot
//   $maxDepth    : INTEGER — Maximum chain depth to traverse (default 6)
//
// PATTERN: The SUBSIDIARY_OF relationship goes from child → parent.
//   (TechNova Solutions)-[:SUBSIDIARY_OF]->(Continental Group)
//                       -[:SUBSIDIARY_OF]->(GlobalHoldings Corp)
//
// ULTIMATE PARENT: A node with no outgoing SUBSIDIARY_OF relationship
//   that is active as of $asOfDate.
// =============================================================================

// ── QUERY 1: Full chain to ultimate parent ──
MATCH path = (c:Company {name: $companyName})
             -[:SUBSIDIARY_OF*1..6]->(ancestor:Company)
WHERE
    -- Bi-temporal: only follow relationships active on asOfDate
    ALL(rel IN relationships(path)
        WHERE rel.effectiveDate <= $asOfDate
          AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate)
    )

WITH path, c, ancestor
    -- Ultimate parent = no active outgoing SUBSIDIARY_OF
WHERE NOT EXISTS {
    MATCH (ancestor)-[exitRel:SUBSIDIARY_OF]->(anyParent:Company)
    WHERE exitRel.effectiveDate <= $asOfDate
      AND (exitRel.expiryDate IS NULL OR exitRel.expiryDate > $asOfDate)
}

RETURN
    c.id                                                              AS entityId,
    c.name                                                            AS entity,
    c.companyType                                                     AS entityType,
    ancestor.id                                                       AS ultimateParentId,
    ancestor.name                                                     AS ultimateParent,
    ancestor.jurisdiction                                             AS ultimateParentJurisdiction,
    length(path)                                                      AS chainDepth,
    [node IN nodes(path) | node.name]                                 AS ownershipChain,
    [rel IN relationships(path) | rel.ownershipPct]                   AS ownershipPercentages,
    [rel IN relationships(path) | rel.ownershipType]                  AS ownershipTypes,
    -- Cumulative effective ownership = product of all ownership percentages
    reduce(pct = 1.0, rel IN relationships(path) |
        pct * coalesce(rel.ownershipPct, 100.0) / 100.0) * 100.0    AS effectiveOwnershipPct
ORDER BY chainDepth ASC;

// =============================================================================
// ── QUERY 2: Find all subsidiaries of a given company (downstream) ──
//    Useful for: "What entities are affected if GlobalHoldings Corp fails?"
// =============================================================================

// MATCH path = (parent:Company {name: $companyName})
//              <-[:SUBSIDIARY_OF*1..6]-(subsidiary:Company)
// WHERE ALL(rel IN relationships(path)
//     WHERE rel.effectiveDate <= $asOfDate
//       AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate)
// )
// RETURN
//     subsidiary.name     AS subsidiary,
//     subsidiary.sector   AS sector,
//     length(path)        AS depth,
//     [n IN nodes(path) | n.name] AS chain
// ORDER BY depth ASC, subsidiary.name ASC;

// =============================================================================
// ── QUERY 3: Historical ownership — what changed between two dates? ──
//    Find relationships that were added or expired in a date range
// =============================================================================

// MATCH (child:Company {name: $companyName})-[rel:SUBSIDIARY_OF]->(parent:Company)
// WHERE rel.effectiveDate >= $fromDate AND rel.effectiveDate <= $toDate
//    OR rel.expiryDate >= $fromDate AND rel.expiryDate <= $toDate
// RETURN
//     child.name          AS child,
//     parent.name         AS parent,
//     rel.ownershipPct    AS ownershipPct,
//     rel.effectiveDate   AS effectiveDate,
//     rel.expiryDate      AS expiryDate,
//     CASE
//         WHEN rel.expiryDate >= $fromDate AND rel.expiryDate <= $toDate THEN 'EXPIRED'
//         ELSE 'CREATED'
//     END AS changeType
// ORDER BY rel.effectiveDate ASC;
