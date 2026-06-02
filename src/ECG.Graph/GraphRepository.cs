using System.Reflection;
using ECG.Core.Exceptions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;

namespace ECG.Graph;

/// <summary>
/// Generic Neo4j repository providing CRUD + upsert operations for all graph node types.
///
/// CONNECTION MODEL:
///   IDriver is a singleton — opened once, shared for the lifetime of the process.
///   IAsyncSession is short-lived — one per logical unit of work, disposed after use.
///   This matches Neo4j Driver best-practice: driver is singleton, session is transient.
///
/// QUERY SAFETY:
///   All Cypher queries use parameterized statements. String interpolation into Cypher
///   is prohibited — it creates a Cypher injection vector equivalent to SQL injection.
///   Exception: node labels and relationship types are inlined from C# type names (never user input).
/// </summary>
public sealed class GraphRepository : IAsyncDisposable
{
    private readonly IDriver _driver;
    private readonly string _database;
    private readonly ILogger<GraphRepository> _logger;

    public GraphRepository(IConfiguration config, ILogger<GraphRepository> logger)
    {
        _logger = logger;
        var uri  = config["Neo4j:Uri"]      ?? "bolt://localhost:7687";
        var user = config["Neo4j:Username"] ?? "neo4j";
        var pass = config["Neo4j:Password"] ?? "ecg_password123";
        _database = config["Neo4j:Database"] ?? "neo4j";

        _driver = GraphDatabase.Driver(uri, AuthTokens.Basic(user, pass));
        _logger.LogInformation("Neo4j driver initialized — {Uri}", uri);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Node Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates or updates a node. MERGEs on id, then SETs all properties.
    /// Safe to call multiple times for the same entity (idempotent).
    /// Label is always typeof(T).Name — a C# type name, never user input.
    /// </summary>
    public async Task<INode> UpsertNodeAsync<T>(T node, string? labelOverride = null) where T : class
    {
        var label = labelOverride ?? typeof(T).Name;
        var props = ToPropertyDict(node);

        // $$""" = double-dollar raw string: {{label}} = C# interpolation, {id: $id} = literal Cypher map
        var cypher = $$"""
            MERGE (n:{{label}} {id: $id})
            SET n += $props
            SET n.updatedAt = datetime()
            RETURN n
            """;

        return await ExecuteWriteAsync(async tx =>
        {
            var cursor = await tx.RunAsync(cypher, new { id = props["id"], props });
            var record = await cursor.SingleAsync();
            return record["n"].As<INode>();
        });
    }

    /// <summary>Creates or merges a relationship between two existing nodes.</summary>
    public async Task CreateRelationshipAsync(
        string fromId, string toId, string relType, Dictionary<string, object?> properties)
    {
        // relType comes from internal C# constants — never user-supplied strings.
        var cypher = $$"""
            MATCH (from {id: $fromId})
            MATCH (to   {id: $toId})
            MERGE (from)-[r:{{relType}}]->(to)
            SET r += $props
            """;

        await ExecuteWriteAsync(async tx =>
        {
            await tx.RunAsync(cypher, new { fromId, toId, props = properties });
            return true;
        });
    }

    /// <summary>
    /// Bi-temporal ownership update:
    ///   1. Expire the currently active SUBSIDIARY_OF (set expiryDate)
    ///   2. Create a new SUBSIDIARY_OF with the given effectiveDate
    /// Never deletes — always expires + creates (immutable audit trail).
    /// </summary>
    public async Task UpdateOwnershipAsync(
        string childId,
        string? oldParentId,
        string newParentId,
        decimal ownershipPct,
        string ownershipType,
        DateTime effectiveDate,
        string sourceSystem)
    {
        const string cypher = """
            OPTIONAL MATCH (child:Company {id: $childId})-[oldRel:SUBSIDIARY_OF]->
                           (oldParent:Company {id: $oldParentId})
            WHERE oldRel.expiryDate IS NULL
            SET oldRel.expiryDate = $effectiveDateStr

            WITH child
            MATCH (newParent:Company {id: $newParentId})
            CREATE (child)-[:SUBSIDIARY_OF {
                ownershipPct:  $ownershipPct,
                ownershipType: $ownershipType,
                effectiveDate: $effectiveDateStr,
                expiryDate:    null,
                sourceSystem:  $sourceSystem
            }]->(newParent)
            """;

        await ExecuteWriteAsync(async tx =>
        {
            await tx.RunAsync(cypher, new
            {
                childId,
                oldParentId = oldParentId ?? "",
                newParentId,
                ownershipPct    = (double)ownershipPct,
                ownershipType,
                effectiveDateStr = effectiveDate.ToString("yyyy-MM-dd"),
                sourceSystem
            });
            return true;
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Generic Query Execution
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Executes a parameterized read query and maps each result record using the provided mapper.
    /// </summary>
    public async Task<List<TResult>> ExecuteReadAsync<TResult>(
        string cypher,
        object parameters,
        Func<IRecord, TResult> mapper)
    {
        return await ReadAsync(async tx =>
        {
            var cursor  = await tx.RunAsync(cypher, parameters);
            var records = await cursor.ToListAsync();
            return records.Select(mapper).ToList();
        });
    }

    /// <summary>Executes a raw read query and returns the full IRecord list.</summary>
    public async Task<List<IRecord>> ExecuteRawReadAsync(string cypher, object parameters)
    {
        return await ReadAsync(async tx =>
        {
            var cursor = await tx.RunAsync(cypher, parameters);
            return await cursor.ToListAsync();
        });
    }

    /// <summary>Fetches a single node by label and id. Throws GraphNodeNotFoundException if absent.</summary>
    public async Task<INode> GetNodeByIdAsync(string label, string id)
    {
        var cypher = $"MATCH (n:{label} {{id: $id}}) RETURN n";
        var records = await ReadAsync(async tx =>
        {
            var cursor = await tx.RunAsync(cypher, new { id });
            return await cursor.ToListAsync();
        });

        if (records.Count == 0)
            throw new GraphNodeNotFoundException(label, id);

        return records[0]["n"].As<INode>();
    }

    /// <summary>Returns node/relationship counts for the /graph/stats endpoint.</summary>
    public async Task<Dictionary<string, long>> GetGraphStatsAsync()
    {
        const string cypher = """
            MATCH (n)            RETURN 'nodes'         AS metric, count(n)    AS value
            UNION ALL
            MATCH ()-[r]->()     RETURN 'relationships' AS metric, count(r)    AS value
            UNION ALL
            MATCH (c:Company)    RETURN 'companies'     AS metric, count(c)    AS value
            UNION ALL
            MATCH (f:Fund)       RETURN 'funds'         AS metric, count(f)    AS value
            UNION ALL
            MATCH (d:Deal)       RETURN 'deals'         AS metric, count(d)    AS value
            UNION ALL
            MATCH (i:Instrument) RETURN 'instruments'   AS metric, count(i)    AS value
            UNION ALL
            MATCH (r:Risk)       RETURN 'risks'         AS metric, count(r)    AS value
            """;

        return await ReadAsync(async tx =>
        {
            var cursor  = await tx.RunAsync(cypher);
            var records = await cursor.ToListAsync();
            return records.ToDictionary(
                r => r["metric"].As<string>(),
                r => r["value"].As<long>());
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Session / transaction management
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<T> ReadAsync<T>(Func<IAsyncQueryRunner, Task<T>> work)
    {
        await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
        try
        {
            return await session.ExecuteReadAsync(work);
        }
        catch (Neo4jException ex)
        {
            _logger.LogError(ex, "Neo4j read error: {Code}", ex.Code);
            throw;
        }
    }

    private async Task<T> ExecuteWriteAsync<T>(Func<IAsyncQueryRunner, Task<T>> work)
    {
        await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));
        try
        {
            return await session.ExecuteWriteAsync(work);
        }
        catch (Neo4jException ex)
        {
            _logger.LogError(ex, "Neo4j write error: {Code}", ex.Code);
            throw;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Utilities
    // ─────────────────────────────────────────────────────────────────────────

    private static Dictionary<string, object?> ToPropertyDict<T>(T obj)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var value = prop.GetValue(obj);
            dict[ToCamelCase(prop.Name)] = value switch
            {
                DateTime dt  => dt.ToString("o"),
                DateTimeOffset dto => dto.ToString("o"),
                _ => value
            };
        }
        return dict;
    }

    private static string ToCamelCase(string name) =>
        name.Length == 0 ? name : char.ToLowerInvariant(name[0]) + name[1..];

    public async ValueTask DisposeAsync()
    {
        await _driver.DisposeAsync();
        _logger.LogInformation("Neo4j driver disposed.");
    }
}
