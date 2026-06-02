namespace ECG.Core.Domain.Relationships;

/// <summary>
/// Base record for every relationship stored in the graph.
///
/// BI-TEMPORAL MODEL:
///   EffectiveDate — when this relationship became true in the real world
///   ExpiryDate    — when this relationship ceased to be true (null = still active)
///
/// IMPORTANT: Relationships are NEVER deleted. When an ownership structure changes,
/// the old relationship is expired (ExpiryDate set) and a new relationship is created.
/// This enables time-travel queries: "What was the ownership graph on 2023-06-01?"
/// </summary>
public abstract record GraphRelationship
{
    public required string FromId { get; init; }
    public required string ToId { get; init; }
    public DateTime EffectiveDate { get; init; } = DateTime.UtcNow;

    /// <summary>Null means the relationship is currently active.</summary>
    public DateTime? ExpiryDate { get; init; }

    public required string SourceSystem { get; init; }
}
