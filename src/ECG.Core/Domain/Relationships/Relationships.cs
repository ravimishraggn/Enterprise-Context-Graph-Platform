namespace ECG.Core.Domain.Relationships;

/// <summary>(Fund)-[HAS_POSITION]->(Instrument)</summary>
public record HasPosition : GraphRelationship
{
    public decimal WeightPct { get; init; }
    public decimal CurrentValueMillions { get; init; }

    /// <summary>ISO-8601 date string — the NAV/valuation date for this position snapshot.</summary>
    public required string AsOfDate { get; init; }
}

/// <summary>(Instrument)-[ISSUED_BY]->(Company)</summary>
public record IssuedBy : GraphRelationship { }

/// <summary>
/// (Company)-[SUBSIDIARY_OF]->(Company)
///
/// This is THE CRITICAL RELATIONSHIP for multi-hop exposure analysis.
/// Direction is: child → parent (mirrors "A is a subsidiary OF B").
///
/// OwnershipType values:
///   DIRECT    — immediate legal ownership
///   INDIRECT  — held through an intermediate entity
///   BENEFICIAL — economic ownership (may differ from legal)
/// </summary>
public record SubsidiaryOf : GraphRelationship
{
    public decimal OwnershipPct { get; init; }
    public required string OwnershipType { get; init; }
}

/// <summary>(Deal)-[INVOLVES]->(Company)</summary>
public record Involves : GraphRelationship
{
    /// <summary>BORROWER | TARGET | SPONSOR | LENDER | ADVISOR | GUARANTOR</summary>
    public required string Role { get; init; }
}

/// <summary>(Deal)-[FINANCED_BY]->(Instrument)</summary>
public record FinancedBy : GraphRelationship
{
    public decimal AllocatedMillions { get; init; }
}

/// <summary>(Fund)-[INVESTED_IN]->(Deal)</summary>
public record InvestedIn : GraphRelationship
{
    public decimal CommittedMillions { get; init; }
    public decimal CalledMillions { get; init; }
}

/// <summary>(Risk)-[ATTACHED_TO]->(Company|Deal|Instrument|Fund)</summary>
public record AttachedTo : GraphRelationship
{
    public required string AttachmentReason { get; init; }
}

/// <summary>(Document)-[RELATES_TO]->(Deal|Company)</summary>
public record RelatesTo : GraphRelationship
{
    /// <summary>PRIMARY | SUPPORTING | HISTORICAL | REGULATORY</summary>
    public required string RelevanceType { get; init; }
}

/// <summary>(Investor)-[LP_IN]->(Fund)</summary>
public record LpIn : GraphRelationship
{
    public decimal CommitmentMillions { get; init; }

    /// <summary>ANCHOR | STANDARD | CO_INVEST | SEPARATELY_MANAGED</summary>
    public required string CommitmentType { get; init; }
}

/// <summary>(Person)-[SERVES_ON_BOARD_OF]->(Company)</summary>
public record ServesOnBoardOf : GraphRelationship
{
    /// <summary>CHAIR | INDEPENDENT | SPONSOR_REP | EXECUTIVE | OBSERVER</summary>
    public required string BoardRole { get; init; }
}
