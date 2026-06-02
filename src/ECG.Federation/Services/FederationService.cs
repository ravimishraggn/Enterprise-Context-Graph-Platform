using ECG.Graph.Services;
using ECG.Graph.Services.Models;
using System.Net.Http.Json;
using System.Text.Json;

namespace ECG.Federation.Services;

/// <summary>
/// Orchestrates fan-out queries across multiple data sources:
///   1. Neo4j graph — structural relationships (always first — graph is the spine)
///   2. Qdrant vector store — related documents by entity ID
///   3. Mock warehouse API — financial metrics (revenue, EBITDA, leverage)
///
/// WHY GRAPH FIRST:
///   The graph query returns canonical entity IDs that are used as keys to query
///   downstream systems. Without the graph, we don't know which entities to enrich.
///   This is the federation spine pattern: graph determines scope, others fill detail.
/// </summary>
public sealed class FederationService
{
    private readonly ExposureQueryService _exposureService;
    private readonly HttpClient _httpClient;
    private readonly ILogger<FederationService> _logger;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public FederationService(
        ExposureQueryService exposureService,
        IHttpClientFactory httpClientFactory,
        ILogger<FederationService> logger)
    {
        _exposureService = exposureService;
        _httpClient = httpClientFactory.CreateClient("WarehouseApi");
        _logger = logger;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Fund Intelligence — full enriched view
    // ─────────────────────────────────────────────────────────────────────────

    public async Task<FundIntelligenceResult> GetFundIntelligenceAsync(
        string fundName,
        string? asOfDate = null)
    {
        _logger.LogInformation("FederationService.GetFundIntelligence: fund={Fund}", fundName);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // ── Step 1: Graph query (blocking — determines entity IDs for step 2+3) ──
        var positions = await _exposureService.GetFundPortfolioAsync(fundName, asOfDate);
        var exposureChains = await _exposureService.GetFundExposureChainAsync(
            fundName, "REGULATORY_WATCH", 4, asOfDate);

        if (positions.Count == 0)
        {
            _logger.LogWarning("No positions found for fund: {Fund}", fundName);
            return new FundIntelligenceResult { FundName = fundName };
        }

        // Extract unique company IDs for parallel enrichment
        var issuerIds = positions.Select(p => p.IssuerId).Distinct().ToList();

        // ── Step 2+3: Parallel enrichment from Qdrant and warehouse ──
        var (documents, metrics) = await (
            GetRelatedDocumentsAsync(issuerIds),
            GetWarehouseMetricsAsync(issuerIds)
        ).WhenAll();

        sw.Stop();
        _logger.LogInformation(
            "FederationService complete: fund={Fund} positions={P} docs={D} metrics={M} elapsed={Ms}ms",
            fundName, positions.Count, documents.Count, metrics.Count, sw.ElapsedMilliseconds);

        return new FundIntelligenceResult
        {
            FundName = fundName,
            FundType = positions.First().FundType,
            AumMillions = positions.First().FundAumMillions,
            AsOfDate = asOfDate ?? DateTime.UtcNow.ToString("yyyy-MM-dd"),
            PositionCount = positions.Count,
            Positions = positions,
            ExposureChains = exposureChains,
            RelatedDocuments = documents,
            WarehouseMetrics = metrics,
            QueryMetadata = new QueryMetadata
            {
                GraphQueryMs = sw.ElapsedMilliseconds,
                DataSources = ["Neo4j Graph", "Qdrant Vector Store", "Financial Data Warehouse"]
            }
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Company Context
    // ─────────────────────────────────────────────────────────────────────────

    public async Task<CompanyContextResult> GetCompanyContextAsync(
        string companyName,
        string? asOfDate = null)
    {
        var chain = await _exposureService.GetOwnershipChainAsync(companyName, asOfDate);
        var metrics = chain is not null
            ? await GetWarehouseMetricsAsync([chain.EntityId])
            : new List<WarehouseMetric>();

        return new CompanyContextResult
        {
            CompanyName = companyName,
            OwnershipChain = chain,
            WarehouseMetrics = metrics
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Downstream enrichment sources
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<List<DocumentResult>> GetRelatedDocumentsAsync(List<string> entityIds)
    {
        try
        {
            // Call Qdrant REST API to find documents with matching entity ID payload filters
            // In production this would use qdrant-client or the Qdrant gRPC channel
            var qdrantUrl = _httpClient.BaseAddress is not null
                ? "/collections/ecg_documents/points/scroll"
                : null;

            if (qdrantUrl is null)
                return MockDocuments(entityIds);

            // Simplified Qdrant scroll with payload filter on entityId field
            var request = new
            {
                filter = new
                {
                    must = new[]
                    {
                        new
                        {
                            key = "entity_id",
                            match = new { any = entityIds }
                        }
                    }
                },
                limit = 20,
                with_payload = true
            };

            var response = await _httpClient.PostAsJsonAsync(qdrantUrl, request);
            if (!response.IsSuccessStatusCode) return MockDocuments(entityIds);

            var body = await response.Content.ReadFromJsonAsync<QdrantScrollResult>();
            return body?.Result?.Points?.Select(p => new DocumentResult
            {
                DocumentId = p.Id?.ToString() ?? "",
                Title = p.Payload.GetValueOrDefault("title")?.ToString() ?? "Unknown",
                DocType = p.Payload.GetValueOrDefault("docType")?.ToString() ?? "UNKNOWN",
                EntityId = p.Payload.GetValueOrDefault("entity_id")?.ToString() ?? "",
                RelevanceScore = p.Score ?? 0
            }).ToList() ?? MockDocuments(entityIds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Qdrant enrichment failed — returning mock documents");
            return MockDocuments(entityIds);
        }
    }

    private async Task<List<WarehouseMetric>> GetWarehouseMetricsAsync(List<string> entityIds)
    {
        var results = new List<WarehouseMetric>();
        var tasks = entityIds.Select(async id =>
        {
            try
            {
                var response = await _httpClient.GetAsync($"/metrics/{id}");
                if (!response.IsSuccessStatusCode) return MockMetric(id);
                var metric = await response.Content.ReadFromJsonAsync<WarehouseMetric>();
                return metric ?? MockMetric(id);
            }
            catch
            {
                return MockMetric(id);
            }
        });

        var fetched = await Task.WhenAll(tasks);
        results.AddRange(fetched.Where(m => m is not null)!);
        return results;
    }

    private static List<DocumentResult> MockDocuments(List<string> entityIds) =>
        entityIds.Take(3).Select((id, i) => new DocumentResult
        {
            DocumentId = $"doc-{id}-{i}",
            Title = $"Annual Report — {id}",
            DocType = i == 0 ? "ANNUAL_REPORT" : i == 1 ? "CIM" : "COVENANT_REPORT",
            EntityId = id,
            RelevanceScore = 0.92 - (i * 0.05)
        }).ToList();

    private static WarehouseMetric MockMetric(string entityId)
    {
        var rng = new Random(entityId.GetHashCode());
        return new WarehouseMetric
        {
            CompanyId = entityId,
            RevenueTtmMillions = Math.Round(50 + rng.NextDouble() * 450, 1),
            EbitdaMargin = Math.Round(0.10 + rng.NextDouble() * 0.25, 3),
            LeverageRatio = Math.Round(3.5 + rng.NextDouble() * 4.5, 2),
            InterestCoverageRatio = Math.Round(1.5 + rng.NextDouble() * 3.5, 2),
            LastUpdated = DateTime.UtcNow.AddDays(-rng.Next(0, 30)).ToString("o")
        };
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Result models
// ─────────────────────────────────────────────────────────────────────────────

public record FundIntelligenceResult
{
    public required string FundName { get; init; }
    public string? FundType { get; init; }
    public decimal AumMillions { get; init; }
    public string? AsOfDate { get; init; }
    public int PositionCount { get; init; }
    public List<FundPortfolioPosition> Positions { get; init; } = [];
    public List<ExposureResult> ExposureChains { get; init; } = [];
    public List<DocumentResult> RelatedDocuments { get; init; } = [];
    public List<WarehouseMetric> WarehouseMetrics { get; init; } = [];
    public QueryMetadata? QueryMetadata { get; init; }
}

public record CompanyContextResult
{
    public required string CompanyName { get; init; }
    public OwnershipChainResult? OwnershipChain { get; init; }
    public List<WarehouseMetric> WarehouseMetrics { get; init; } = [];
}

public record DocumentResult
{
    public required string DocumentId { get; init; }
    public required string Title { get; init; }
    public required string DocType { get; init; }
    public required string EntityId { get; init; }
    public double RelevanceScore { get; init; }
}

public record WarehouseMetric
{
    public required string CompanyId { get; init; }
    public double RevenueTtmMillions { get; init; }
    public double EbitdaMargin { get; init; }
    public double LeverageRatio { get; init; }
    public double InterestCoverageRatio { get; init; }
    public required string LastUpdated { get; init; }
}

public record QueryMetadata
{
    public long GraphQueryMs { get; init; }
    public List<string> DataSources { get; init; } = [];
}

// Qdrant response deserialization helpers
internal record QdrantScrollResult
{
    public QdrantScrollData? Result { get; init; }
}

internal record QdrantScrollData
{
    public List<QdrantPoint>? Points { get; init; }
}

internal record QdrantPoint
{
    public object Id { get; init; } = "";
    public Dictionary<string, object> Payload { get; init; } = new();
    public double? Score { get; init; }
}

// WhenAll helper for value tuples
internal static class TaskExtensions
{
    public static async Task<(T1, T2)> WhenAll<T1, T2>(this (Task<T1>, Task<T2>) tasks)
    {
        await Task.WhenAll(tasks.Item1, tasks.Item2);
        return (tasks.Item1.Result, tasks.Item2.Result);
    }
}
