namespace ECG.Core.Exceptions;

/// <summary>
/// Thrown when a graph traversal or lookup targets a node that does not exist.
/// Callers should distinguish this from a connectivity error (Neo4jDriverException).
/// </summary>
public sealed class GraphNodeNotFoundException : Exception
{
    public string NodeLabel { get; }
    public string NodeId { get; }

    public GraphNodeNotFoundException(string nodeLabel, string nodeId)
        : base($"Graph node not found: [{nodeLabel}] id='{nodeId}'")
    {
        NodeLabel = nodeLabel;
        NodeId = nodeId;
    }

    public GraphNodeNotFoundException(string nodeLabel, string nodeId, Exception inner)
        : base($"Graph node not found: [{nodeLabel}] id='{nodeId}'", inner)
    {
        NodeLabel = nodeLabel;
        NodeId = nodeId;
    }
}

/// <summary>
/// Thrown when a variable-length traversal exceeds the configured time budget.
/// Use maxHops to bound depth; use this exception to surface unbounded queries.
/// </summary>
public sealed class TraversalTimeoutException : Exception
{
    public string QueryName { get; }
    public int MaxHops { get; }
    public TimeSpan Elapsed { get; }

    public TraversalTimeoutException(string queryName, int maxHops, TimeSpan elapsed)
        : base($"Graph traversal timed out: query='{queryName}' maxHops={maxHops} elapsed={elapsed.TotalSeconds:F1}s")
    {
        QueryName = queryName;
        MaxHops = maxHops;
        Elapsed = elapsed;
    }
}

/// <summary>
/// Thrown when entity resolution fails to find a canonical ID for an inbound entity name.
/// Indicates the entity resolution service needs a new mapping, not a graph data error.
/// </summary>
public sealed class EntityResolutionException : Exception
{
    public string RawEntityName { get; }
    public string SourceSystem { get; }

    public EntityResolutionException(string rawEntityName, string sourceSystem)
        : base($"Entity resolution failed: name='{rawEntityName}' from system='{sourceSystem}'")
    {
        RawEntityName = rawEntityName;
        SourceSystem = sourceSystem;
    }
}

/// <summary>
/// Thrown when a graph mutation event arrives with an invalid or inconsistent state,
/// e.g., an ownership change references a company that doesn't exist in the graph.
/// </summary>
public sealed class GraphMutationException : Exception
{
    public string EventId { get; }
    public string EventType { get; }

    public GraphMutationException(string eventId, string eventType, string message)
        : base($"Graph mutation failed: event='{eventId}' type='{eventType}' — {message}")
    {
        EventId = eventId;
        EventType = eventType;
    }

    public GraphMutationException(string eventId, string eventType, string message, Exception inner)
        : base($"Graph mutation failed: event='{eventId}' type='{eventType}' — {message}", inner)
    {
        EventId = eventId;
        EventType = eventType;
    }
}
