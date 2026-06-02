namespace ECG.Core.Domain.Nodes;

/// <summary>
/// Base record for every node stored in the graph.
/// All properties map 1:1 to Neo4j node properties (camelCase in Cypher).
/// </summary>
public abstract record GraphNode
{
    public required string Id { get; init; }
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Originating system identifier.
    /// Possible values: LOAN_MGMT, PORTFOLIO_MON, MARKET_DATA, MANUAL, API
    /// </summary>
    public required string SourceSystem { get; init; }
}
