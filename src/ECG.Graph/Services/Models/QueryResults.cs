namespace ECG.Graph.Services.Models;

/// <summary>
/// Represents a single row in a multi-hop exposure analysis:
/// Fund → Instrument → Issuer → (Subsidiary Chain) → Risk-bearing Entity ← Risk
/// </summary>
public record ExposureResult
{
    public required string FundId { get; init; }
    public required string Fund { get; init; }
    public decimal FundAumMillions { get; init; }
    public required string FundType { get; init; }
    public required string DirectIssuer { get; init; }
    public required string IssuerSector { get; init; }
    public required string RiskBearingEntity { get; init; }
    public required string RiskJurisdiction { get; init; }
    public int TotalHops { get; init; }
    public int OwnershipHops { get; init; }
    public required string RiskId { get; init; }
    public required string RiskType { get; init; }
    public required string Severity { get; init; }
    public required string RiskDescription { get; init; }
    public string? RiskFlaggedDate { get; init; }
    public required string FlaggedBy { get; init; }
    public decimal PositionValueMillions { get; init; }
    public decimal PositionWeightPct { get; init; }
    public string? InstrumentIsin { get; init; }
    public required string InstrumentType { get; init; }
    public List<string> ExposurePath { get; init; } = [];
    public List<decimal?> OwnershipChain { get; init; } = [];
}

/// <summary>Full ownership chain from an entity to its ultimate beneficial owner.</summary>
public record OwnershipChainResult
{
    public required string EntityId { get; init; }
    public required string Entity { get; init; }
    public required string EntityType { get; init; }
    public required string UltimateParentId { get; init; }
    public required string UltimateParent { get; init; }
    public required string UltimateParentJurisdiction { get; init; }
    public int ChainDepth { get; init; }
    public List<string> OwnershipChain { get; init; } = [];
    public List<decimal?> OwnershipPercentages { get; init; } = [];
    public List<string?> OwnershipTypes { get; init; } = [];
    public decimal EffectiveOwnershipPct { get; init; }
}

/// <summary>One affected entity in a risk propagation blast-radius analysis.</summary>
public record RiskPropagationResult
{
    public required string RiskType { get; init; }
    public required string Severity { get; init; }
    public required string RiskDescription { get; init; }
    public required string RiskOriginEntity { get; init; }
    public required string OriginEntityType { get; init; }
    public required string AffectedCompany { get; init; }
    public required string AffectedSector { get; init; }
    public required string AffectedJurisdiction { get; init; }
    public int OwnershipDepth { get; init; }
    public string? InstrumentIsin { get; init; }
    public string? InstrumentType { get; init; }
    public decimal? InstrumentFaceValue { get; init; }
    public decimal? CurrentPositionValue { get; init; }
    public decimal? PortfolioWeightPct { get; init; }
    public string? AffectedFund { get; init; }
    public decimal? FundAum { get; init; }
    public string? RelatedDeal { get; init; }
    public string? DealType { get; init; }
    public string? DealStatus { get; init; }
}

/// <summary>Full portfolio view for a fund — one row per position with deal + risk context.</summary>
public record FundPortfolioPosition
{
    public required string Fund { get; init; }
    public required string FundType { get; init; }
    public decimal FundAumMillions { get; init; }
    public required string InstrumentId { get; init; }
    public string? Isin { get; init; }
    public string? Cusip { get; init; }
    public required string InstrumentType { get; init; }
    public decimal FaceValueMillions { get; init; }
    public decimal? Coupon { get; init; }
    public string? MaturityDate { get; init; }
    public required string Currency { get; init; }
    public decimal CurrentValueMillions { get; init; }
    public decimal PortfolioWeightPct { get; init; }
    public required string IssuerId { get; init; }
    public required string Issuer { get; init; }
    public required string IssuerSector { get; init; }
    public required string IssuerJurisdiction { get; init; }
    public required string IssuerType { get; init; }
    public string? DealName { get; init; }
    public string? DealType { get; init; }
    public string? DealStatus { get; init; }
    public decimal? DealSizeMillions { get; init; }
    public List<RiskOverlay> DirectRisks { get; init; } = [];
    public List<RiskOverlay> IndirectRisks { get; init; } = [];
    public required string WorstRiskSeverity { get; init; }
}

public record RiskOverlay
{
    public string? RiskId { get; init; }
    public string? RiskType { get; init; }
    public string? Severity { get; init; }
    public string? Description { get; init; }
    public string? FlaggedDate { get; init; }
    public string? AttachedTo { get; init; }
    public string? HopsFromIssuer { get; init; }
}

/// <summary>Aggregate graph statistics returned by /api/graph/stats</summary>
public record GraphStats
{
    public long TotalNodes { get; init; }
    public long TotalRelationships { get; init; }
    public long Companies { get; init; }
    public long Funds { get; init; }
    public long Deals { get; init; }
    public long Instruments { get; init; }
    public long Risks { get; init; }
    public DateTime GeneratedAt { get; init; } = DateTime.UtcNow;
}
