namespace ECG.Core.Events;

/// <summary>
/// Base record for all ECG Kafka events.
/// EventId is used as the Kafka message key for deduplication.
/// </summary>
public abstract record GraphEvent
{
    public required string EventId { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public required string SchemaVersion { get; init; }
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: ecg.deal.created
// Partition key: DealId (ensures all events for a deal are ordered)
// ─────────────────────────────────────────────────────────────────────────────
public record DealCreatedEvent : GraphEvent
{
    public required string DealId { get; init; }
    public required string DealName { get; init; }
    public required string DealType { get; init; }
    public decimal DealSizeMillions { get; init; }
    public required string Currency { get; init; }
    public required string BorrowerCompanyId { get; init; }
    public required string SponsorCompanyId { get; init; }

    /// <summary>Fund IDs participating in this deal's financing.</summary>
    public List<string> FundIds { get; init; } = [];

    public required string Status { get; init; }
    public DateTime? CloseDate { get; init; }
    public required string SourceSystem { get; init; }
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: ecg.deal.status.changed
// Partition key: DealId
// ─────────────────────────────────────────────────────────────────────────────
public record DealStatusChangedEvent : GraphEvent
{
    public required string DealId { get; init; }
    public required string OldStatus { get; init; }
    public required string NewStatus { get; init; }
    public DateTime EffectiveDate { get; init; }
    public string? ChangeNote { get; init; }
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: ecg.ownership.changed
// Partition key: ChildCompanyId (canonical entity ID — ensures ordering per entity)
//
// WHY ENTITY ID NOT EVENT ID: Ownership changes for the same entity must be
// processed in order to maintain correct bi-temporal state. Two concurrent
// restructuring events for the same subsidiary on different partitions would
// create an inconsistent ownership graph.
// ─────────────────────────────────────────────────────────────────────────────
public record OwnershipChangedEvent : GraphEvent
{
    public required string ChildCompanyId { get; init; }
    public string? OldParentCompanyId { get; init; }
    public required string NewParentCompanyId { get; init; }
    public decimal NewOwnershipPct { get; init; }
    public required string OwnershipType { get; init; }

    /// <summary>ACQUISITION | RESTRUCTURING | DIVESTITURE | IPO | BANKRUPTCY</summary>
    public required string ChangeReason { get; init; }

    /// <summary>The real-world effective date of the change (may precede event timestamp).</summary>
    public DateTime EffectiveDate { get; init; }
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: ecg.risk.flagged
// Partition key: AttachedToEntityId (groups risk events by the entity they affect)
// ─────────────────────────────────────────────────────────────────────────────
public record RiskFlaggedEvent : GraphEvent
{
    public required string RiskId { get; init; }

    /// <summary>CREDIT | REGULATORY_WATCH | OPERATIONAL | MARKET | LIQUIDITY</summary>
    public required string RiskType { get; init; }

    /// <summary>HIGH | MEDIUM | LOW</summary>
    public required string Severity { get; init; }

    public required string AttachedToEntityId { get; init; }

    /// <summary>Company | Deal | Instrument | Fund</summary>
    public required string AttachedToEntityType { get; init; }

    public required string Description { get; init; }

    /// <summary>SYSTEM | ANALYST | REGULATORY_FEED | MARKET_DATA</summary>
    public required string FlaggedBy { get; init; }
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: ecg.valuation.updated
// Partition key: InstrumentId
// ─────────────────────────────────────────────────────────────────────────────
public record ValuationUpdatedEvent : GraphEvent
{
    public required string InstrumentId { get; init; }
    public required string FundId { get; init; }
    public decimal PreviousValueMillions { get; init; }
    public decimal NewValueMillions { get; init; }
    public decimal NewWeightPct { get; init; }
    public required string AsOfDate { get; init; }
    public required string ValuationSource { get; init; }
}
