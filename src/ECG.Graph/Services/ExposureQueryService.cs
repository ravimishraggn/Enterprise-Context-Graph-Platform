using ECG.Graph.Services.Models;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;

namespace ECG.Graph.Services;

/// <summary>
/// Executes the core capital markets graph queries against Neo4j.
/// All methods are parameterized — no string interpolation into Cypher.
/// </summary>
public sealed class ExposureQueryService
{
    private readonly GraphRepository _repo;
    private readonly ILogger<ExposureQueryService> _logger;

    // Default snapshot date used when caller does not specify
    private static readonly string DefaultAsOfDate =
        DateTime.UtcNow.ToString("yyyy-MM-dd");

    public ExposureQueryService(GraphRepository repo, ILogger<ExposureQueryService> logger)
    {
        _repo = repo;
        _logger = logger;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Multi-hop indirect exposure (Fund → … → Risk)
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns all indirect exposure chains from a fund to risks of a given type.
    /// Traverses up to maxHops SUBSIDIARY_OF hops in the ownership chain.
    /// </summary>
    public async Task<List<ExposureResult>> GetFundExposureChainAsync(
        string fundName,
        string riskType = "REGULATORY_WATCH",
        int maxHops = 4,
        string? asOfDate = null)
    {
        asOfDate ??= DefaultAsOfDate;
        _logger.LogInformation(
            "ExposureQuery: fund={Fund} riskType={RiskType} maxHops={MaxHops} asOf={AsOf}",
            fundName, riskType, maxHops, asOfDate);

        // Dynamic max-hops are safe to inline as integer literals — not user string input.
        // Use $$""" so {name: $fundName} = literal Cypher map and {{maxHops}} = C# interpolation.
        var cypher = $$"""
            MATCH path = (f:Fund {name: $fundName})
                         -[pos:HAS_POSITION]->(i:Instrument)
                         -[:ISSUED_BY]->(directIssuer:Company)
                         -[:SUBSIDIARY_OF*1..{{maxHops}}]->(riskEntity:Company)
                         <-[:ATTACHED_TO]-(r:Risk)
            WHERE r.riskType = $riskType
              AND r.isActive = true
              AND r.severity IN ['HIGH', 'MEDIUM']
              AND ALL(rel IN relationships(path)
                  WHERE type(rel) <> 'SUBSIDIARY_OF'
                     OR (rel.effectiveDate <= $asOfDate
                         AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate)))
            RETURN
                f.id                                                         AS fundId,
                f.name                                                       AS fund,
                f.aumMillions                                                AS fundAumMillions,
                f.fundType                                                   AS fundType,
                directIssuer.name                                            AS directIssuer,
                directIssuer.sector                                          AS issuerSector,
                riskEntity.name                                              AS riskBearingEntity,
                riskEntity.jurisdiction                                      AS riskJurisdiction,
                length(path)                                                 AS totalHops,
                length(path) - 2                                             AS ownershipHops,
                r.id                                                         AS riskId,
                r.riskType                                                   AS riskType,
                r.severity                                                   AS severity,
                r.description                                                AS riskDescription,
                r.flaggedDate                                                AS riskFlaggedDate,
                r.flaggedBy                                                  AS flaggedBy,
                pos.currentValueMillions                                     AS positionValueMillions,
                pos.weightPct                                                AS positionWeightPct,
                i.isin                                                       AS instrumentIsin,
                i.instrumentType                                             AS instrumentType,
                [n IN nodes(path) | coalesce(n.name, n.dealName)]           AS exposurePath,
                [rel IN relationships(path)
                    WHERE type(rel) = 'SUBSIDIARY_OF' | rel.ownershipPct]   AS ownershipChain
            ORDER BY r.severity DESC, totalHops ASC
            """;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var results = await _repo.ExecuteReadAsync(
            cypher,
            new { fundName, riskType, asOfDate },
            MapExposureResult);
        sw.Stop();

        _logger.LogInformation(
            "ExposureQuery complete: {Count} paths found in {Ms}ms",
            results.Count, sw.ElapsedMilliseconds);

        return results;
    }

    /// <summary>
    /// Returns all funds with ANY indirect exposure to a risk type, across the entire portfolio.
    /// Useful for portfolio-level risk dashboards.
    /// </summary>
    public async Task<List<ExposureResult>> FindIndirectExposureByRiskTypeAsync(
        string riskType,
        string? asOfDate = null)
    {
        asOfDate ??= DefaultAsOfDate;

        const string cypher = """
            MATCH path = (f:Fund)-[pos:HAS_POSITION]->(i:Instrument)
                         -[:ISSUED_BY]->(directIssuer:Company)
                         -[:SUBSIDIARY_OF*1..5]->(riskEntity:Company)
                         <-[:ATTACHED_TO]-(r:Risk)
            WHERE r.riskType = $riskType
              AND r.isActive = true
              AND ALL(rel IN relationships(path)
                  WHERE type(rel) <> 'SUBSIDIARY_OF'
                     OR (rel.effectiveDate <= $asOfDate
                         AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate)))
            RETURN
                f.id                                                         AS fundId,
                f.name                                                       AS fund,
                f.aumMillions                                                AS fundAumMillions,
                f.fundType                                                   AS fundType,
                directIssuer.name                                            AS directIssuer,
                directIssuer.sector                                          AS issuerSector,
                riskEntity.name                                              AS riskBearingEntity,
                riskEntity.jurisdiction                                      AS riskJurisdiction,
                length(path)                                                 AS totalHops,
                length(path) - 2                                             AS ownershipHops,
                r.id                                                         AS riskId,
                r.riskType                                                   AS riskType,
                r.severity                                                   AS severity,
                r.description                                                AS riskDescription,
                r.flaggedDate                                                AS riskFlaggedDate,
                r.flaggedBy                                                  AS flaggedBy,
                pos.currentValueMillions                                     AS positionValueMillions,
                pos.weightPct                                                AS positionWeightPct,
                i.isin                                                       AS instrumentIsin,
                i.instrumentType                                             AS instrumentType,
                [n IN nodes(path) | coalesce(n.name, n.dealName)]           AS exposurePath,
                [rel IN relationships(path)
                    WHERE type(rel) = 'SUBSIDIARY_OF' | rel.ownershipPct]   AS ownershipChain
            ORDER BY r.severity DESC, f.aumMillions DESC
            """;

        return await _repo.ExecuteReadAsync(
            cypher, new { riskType, asOfDate }, MapExposureResult);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Ownership chain traversal
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Traces the full ownership chain from a company to its ultimate parent.</summary>
    public async Task<OwnershipChainResult?> GetOwnershipChainAsync(
        string companyName,
        string? asOfDate = null)
    {
        asOfDate ??= DefaultAsOfDate;

        const string cypher = """
            MATCH path = (c:Company {name: $companyName})
                         -[:SUBSIDIARY_OF*1..6]->(ancestor:Company)
            WHERE ALL(rel IN relationships(path)
                WHERE rel.effectiveDate <= $asOfDate
                  AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate))
            WITH path, c, ancestor
            WHERE NOT EXISTS {
                MATCH (ancestor)-[exitRel:SUBSIDIARY_OF]->(anyParent:Company)
                WHERE exitRel.effectiveDate <= $asOfDate
                  AND (exitRel.expiryDate IS NULL OR exitRel.expiryDate > $asOfDate)
            }
            RETURN
                c.id                                                          AS entityId,
                c.name                                                        AS entity,
                c.companyType                                                 AS entityType,
                ancestor.id                                                   AS ultimateParentId,
                ancestor.name                                                 AS ultimateParent,
                ancestor.jurisdiction                                         AS ultimateParentJurisdiction,
                length(path)                                                  AS chainDepth,
                [node IN nodes(path) | node.name]                             AS ownershipChain,
                [rel IN relationships(path) | rel.ownershipPct]               AS ownershipPercentages,
                [rel IN relationships(path) | rel.ownershipType]              AS ownershipTypes,
                reduce(pct = 1.0, rel IN relationships(path) |
                    pct * coalesce(rel.ownershipPct, 100.0) / 100.0) * 100.0 AS effectiveOwnershipPct
            ORDER BY chainDepth ASC
            LIMIT 1
            """;

        var rows = await _repo.ExecuteReadAsync(
            cypher,
            new { companyName, asOfDate },
            r => new OwnershipChainResult
            {
                EntityId = r["entityId"].As<string>() ?? "",
                Entity = r["entity"].As<string>() ?? "",
                EntityType = r["entityType"].As<string>() ?? "",
                UltimateParentId = r["ultimateParentId"].As<string>() ?? "",
                UltimateParent = r["ultimateParent"].As<string>() ?? "",
                UltimateParentJurisdiction = r["ultimateParentJurisdiction"].As<string>() ?? "",
                ChainDepth = (int)r["chainDepth"].As<long>(),
                OwnershipChain = r["ownershipChain"].As<List<object>>()
                    .Select(x => x?.ToString() ?? "").ToList(),
                OwnershipPercentages = r["ownershipPercentages"].As<List<object>>()
                    .Select(x => x == null ? (decimal?)null : Convert.ToDecimal(x)).ToList(),
                OwnershipTypes = r["ownershipTypes"].As<List<object>>()
                    .Select(x => x?.ToString()).ToList(),
                EffectiveOwnershipPct = Convert.ToDecimal(r["effectiveOwnershipPct"].As<double>())
            });

        return rows.FirstOrDefault();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Risk propagation blast radius
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Returns all entities, instruments, and funds in the blast radius of a risk.</summary>
    public async Task<List<RiskPropagationResult>> GetRiskPropagationAsync(
        string riskId,
        string? asOfDate = null)
    {
        asOfDate ??= DefaultAsOfDate;

        const string cypher = """
            MATCH (r:Risk {id: $riskId})-[:ATTACHED_TO]->(origin)
            MATCH subsidPath = (origin)<-[:SUBSIDIARY_OF*0..5]-(affectedEntity:Company)
            WHERE ALL(rel IN relationships(subsidPath)
                WHERE rel.effectiveDate <= $asOfDate
                  AND (rel.expiryDate IS NULL OR rel.expiryDate > $asOfDate))
            OPTIONAL MATCH (instrument:Instrument)-[:ISSUED_BY]->(affectedEntity)
            OPTIONAL MATCH (fund:Fund)-[pos:HAS_POSITION]->(instrument)
            OPTIONAL MATCH (deal:Deal)-[:INVOLVES]->(affectedEntity)
            WHERE deal.status IN ['CLOSED', 'PIPELINE']
            RETURN
                r.riskType                      AS riskType,
                r.severity                      AS severity,
                r.description                   AS riskDescription,
                labels(origin)[0]               AS originEntityType,
                coalesce(origin.name, origin.dealName) AS riskOriginEntity,
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
            ORDER BY r.severity DESC, ownershipDepth ASC, pos.currentValueMillions DESC
            """;

        return await _repo.ExecuteReadAsync(
            cypher,
            new { riskId, asOfDate },
            r => new RiskPropagationResult
            {
                RiskType = r["riskType"].As<string>() ?? "",
                Severity = r["severity"].As<string>() ?? "",
                RiskDescription = r["riskDescription"].As<string>() ?? "",
                OriginEntityType = r["originEntityType"].As<string>() ?? "",
                RiskOriginEntity = r["riskOriginEntity"].As<string>() ?? "",
                AffectedCompany = r["affectedCompany"].As<string>() ?? "",
                AffectedSector = r["affectedSector"].As<string>() ?? "",
                AffectedJurisdiction = r["affectedJurisdiction"].As<string>() ?? "",
                OwnershipDepth = (int)r["ownershipDepth"].As<long>(),
                InstrumentIsin = r["instrumentIsin"].As<string?>(),
                InstrumentType = r["instrumentType"].As<string?>(),
                InstrumentFaceValue = r["instrumentFaceValue"].As<double?>() is { } fv ? (decimal?)Convert.ToDecimal(fv) : null,
                CurrentPositionValue = r["currentPositionValue"].As<double?>() is { } cv ? (decimal?)Convert.ToDecimal(cv) : null,
                PortfolioWeightPct = r["portfolioWeightPct"].As<double?>() is { } wt ? (decimal?)Convert.ToDecimal(wt) : null,
                AffectedFund = r["affectedFund"].As<string?>(),
                FundAum = r["fundAum"].As<double?>() is { } fa ? (decimal?)Convert.ToDecimal(fa) : null,
                RelatedDeal = r["relatedDeal"].As<string?>(),
                DealType = r["dealType"].As<string?>(),
                DealStatus = r["dealStatus"].As<string?>()
            });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Full portfolio view
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Returns all portfolio positions for a fund with deal + risk overlay.</summary>
    public async Task<List<FundPortfolioPosition>> GetFundPortfolioAsync(
        string fundName,
        string? asOfDate = null)
    {
        asOfDate ??= DefaultAsOfDate;

        const string cypher = """
            MATCH (f:Fund {name: $fundName})-[pos:HAS_POSITION]->(i:Instrument)-[:ISSUED_BY]->(c:Company)
            OPTIONAL MATCH (d:Deal)-[:FINANCED_BY]->(i)
            OPTIONAL MATCH (r:Risk)-[:ATTACHED_TO]->(c)
            WHERE r.isActive = true
            WITH f, pos, i, c, d,
                 collect(DISTINCT {riskId: r.id, riskType: r.riskType, severity: r.severity,
                                   description: r.description, flaggedDate: r.flaggedDate}) AS directRisks
            OPTIONAL MATCH (pr:Risk)-[:ATTACHED_TO]->(parent:Company)
                          <-[:SUBSIDIARY_OF*1..3]-(c)
            WHERE pr.isActive = true
            WITH f, pos, i, c, d, directRisks,
                 collect(DISTINCT {riskId: pr.id, riskType: pr.riskType, severity: pr.severity,
                                   description: pr.description, flaggedDate: pr.flaggedDate,
                                   attachedTo: parent.name, hopsFromIssuer: 'INDIRECT'}) AS indirectRisks
            RETURN
                f.name                  AS fund,
                f.fundType              AS fundType,
                f.aumMillions           AS fundAumMillions,
                i.id                    AS instrumentId,
                i.isin                  AS isin,
                i.cusip                 AS cusip,
                i.instrumentType        AS instrumentType,
                i.faceValueMillions     AS faceValueMillions,
                i.coupon                AS coupon,
                i.maturityDate          AS maturityDate,
                i.currency              AS currency,
                pos.currentValueMillions AS currentValueMillions,
                pos.weightPct           AS portfolioWeightPct,
                c.id                    AS issuerId,
                c.name                  AS issuer,
                c.sector                AS issuerSector,
                c.jurisdiction          AS issuerJurisdiction,
                c.companyType           AS issuerType,
                d.dealName              AS dealName,
                d.dealType              AS dealType,
                d.status                AS dealStatus,
                d.dealSizeMillions      AS dealSizeMillions,
                directRisks,
                indirectRisks,
                CASE
                    WHEN any(risk IN directRisks WHERE risk.severity = 'HIGH')
                      OR any(risk IN indirectRisks WHERE risk.severity = 'HIGH') THEN 'HIGH'
                    WHEN any(risk IN directRisks WHERE risk.severity = 'MEDIUM')
                      OR any(risk IN indirectRisks WHERE risk.severity = 'MEDIUM') THEN 'MEDIUM'
                    WHEN any(risk IN directRisks WHERE risk.severity = 'LOW')
                      OR any(risk IN indirectRisks WHERE risk.severity = 'LOW') THEN 'LOW'
                    ELSE 'NONE'
                END AS worstRiskSeverity
            ORDER BY pos.currentValueMillions DESC
            """;

        return await _repo.ExecuteReadAsync(
            cypher,
            new { fundName },
            r => new FundPortfolioPosition
            {
                Fund = r["fund"].As<string>() ?? "",
                FundType = r["fundType"].As<string>() ?? "",
                FundAumMillions = Convert.ToDecimal(r["fundAumMillions"].As<double>()),
                InstrumentId = r["instrumentId"].As<string>() ?? "",
                Isin = r["isin"].As<string?>(),
                Cusip = r["cusip"].As<string?>(),
                InstrumentType = r["instrumentType"].As<string>() ?? "",
                FaceValueMillions = Convert.ToDecimal(r["faceValueMillions"].As<double>()),
                Coupon = r["coupon"].As<double?>() is { } c ? (decimal?)Convert.ToDecimal(c) : null,
                MaturityDate = r["maturityDate"].As<string?>(),
                Currency = r["currency"].As<string>() ?? "",
                CurrentValueMillions = Convert.ToDecimal(r["currentValueMillions"].As<double>()),
                PortfolioWeightPct = Convert.ToDecimal(r["portfolioWeightPct"].As<double>()),
                IssuerId = r["issuerId"].As<string>() ?? "",
                Issuer = r["issuer"].As<string>() ?? "",
                IssuerSector = r["issuerSector"].As<string>() ?? "",
                IssuerJurisdiction = r["issuerJurisdiction"].As<string>() ?? "",
                IssuerType = r["issuerType"].As<string>() ?? "",
                DealName = r["dealName"].As<string?>(),
                DealType = r["dealType"].As<string?>(),
                DealStatus = r["dealStatus"].As<string?>(),
                DealSizeMillions = r["dealSizeMillions"].As<double?>() is { } ds ? (decimal?)Convert.ToDecimal(ds) : null,
                DirectRisks = MapRiskOverlays(r["directRisks"]),
                IndirectRisks = MapRiskOverlays(r["indirectRisks"]),
                WorstRiskSeverity = r["worstRiskSeverity"].As<string>() ?? "NONE"
            });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private mappers
    // ─────────────────────────────────────────────────────────────────────────

    private static ExposureResult MapExposureResult(IRecord r) => new()
    {
        FundId = r["fundId"].As<string>() ?? "",
        Fund = r["fund"].As<string>() ?? "",
        FundAumMillions = Convert.ToDecimal(r["fundAumMillions"].As<double>()),
        FundType = r["fundType"].As<string>() ?? "",
        DirectIssuer = r["directIssuer"].As<string>() ?? "",
        IssuerSector = r["issuerSector"].As<string>() ?? "",
        RiskBearingEntity = r["riskBearingEntity"].As<string>() ?? "",
        RiskJurisdiction = r["riskJurisdiction"].As<string>() ?? "",
        TotalHops = (int)r["totalHops"].As<long>(),
        OwnershipHops = (int)r["ownershipHops"].As<long>(),
        RiskId = r["riskId"].As<string>() ?? "",
        RiskType = r["riskType"].As<string>() ?? "",
        Severity = r["severity"].As<string>() ?? "",
        RiskDescription = r["riskDescription"].As<string>() ?? "",
        RiskFlaggedDate = r["riskFlaggedDate"].As<string?>(),
        FlaggedBy = r["flaggedBy"].As<string>() ?? "",
        PositionValueMillions = Convert.ToDecimal(r["positionValueMillions"].As<double>()),
        PositionWeightPct = Convert.ToDecimal(r["positionWeightPct"].As<double>()),
        InstrumentIsin = r["instrumentIsin"].As<string?>(),
        InstrumentType = r["instrumentType"].As<string>() ?? "",
        ExposurePath = r["exposurePath"].As<List<object>>()
            .Select(x => x?.ToString() ?? "").ToList(),
        OwnershipChain = r["ownershipChain"].As<List<object>>()
            .Select(x => x == null ? (decimal?)null : (decimal?)Convert.ToDecimal(x)).ToList()
    };

    private static List<RiskOverlay> MapRiskOverlays(object rawValue)
    {
        if (rawValue is not List<object> list) return [];
        return list
            .OfType<Dictionary<string, object>>()
            .Where(d => d.ContainsKey("riskId") && d["riskId"] is not null)
            .Select(d => new RiskOverlay
            {
                RiskId = d.GetValueOrDefault("riskId")?.ToString(),
                RiskType = d.GetValueOrDefault("riskType")?.ToString(),
                Severity = d.GetValueOrDefault("severity")?.ToString(),
                Description = d.GetValueOrDefault("description")?.ToString(),
                FlaggedDate = d.GetValueOrDefault("flaggedDate")?.ToString(),
                AttachedTo = d.GetValueOrDefault("attachedTo")?.ToString(),
                HopsFromIssuer = d.GetValueOrDefault("hopsFromIssuer")?.ToString()
            })
            .ToList();
    }
}
