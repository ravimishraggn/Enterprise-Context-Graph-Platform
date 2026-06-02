using ECG.Core.Exceptions;
using ECG.Graph;
using ECG.Graph.Services;
using ECG.Graph.Services.Models;
using Microsoft.AspNetCore.Mvc;
using Serilog;
using Serilog.Events;
using System.Text.Json;
using System.Text.Json.Serialization;

// ─────────────────────────────────────────────────────────────────────────────
// Bootstrap Serilog early so startup errors are captured
// ─────────────────────────────────────────────────────────────────────────────
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate:
        "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}")
    .WriteTo.File("logs/ecg-api.log", rollingInterval: RollingInterval.Day, retainedFileCountLimit: 7)
    .CreateLogger();

try
{
    Log.Information("ECG API starting up...");

    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog();

    // ─────────────────────────────────────────────────────────────────────────
    // Services
    // ─────────────────────────────────────────────────────────────────────────
    builder.Services.AddSingleton<GraphRepository>();
    builder.Services.AddScoped<ExposureQueryService>();

    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen(c =>
    {
        c.SwaggerDoc("v1", new()
        {
            Title = "Enterprise Context Graph (ECG) API",
            Version = "v1",
            Description = """
                Graph-native API for capital markets entity relationships and multi-hop exposure analysis.
                All traversal queries are parameterized Cypher — no string interpolation.
                """
        });
    });

    builder.Services.AddProblemDetails();

    builder.Services.ConfigureHttpJsonOptions(opts =>
    {
        opts.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
        opts.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        opts.SerializerOptions.WriteIndented = true;
    });

    // ─────────────────────────────────────────────────────────────────────────
    // Pipeline
    // ─────────────────────────────────────────────────────────────────────────
    var app = builder.Build();

    app.UseExceptionHandler();
    app.UseSerilogRequestLogging(opts =>
    {
        opts.MessageTemplate = "HTTP {RequestMethod} {RequestPath} → {StatusCode} in {Elapsed:0.0}ms";
    });

    if (app.Environment.IsDevelopment())
    {
        app.UseSwagger();
        app.UseSwaggerUI(c =>
        {
            c.SwaggerEndpoint("/swagger/v1/swagger.json", "ECG API v1");
            c.RoutePrefix = "swagger";
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Health check
    // ─────────────────────────────────────────────────────────────────────────
    app.MapGet("/health", () => Results.Ok(new
    {
        status = "healthy",
        timestamp = DateTime.UtcNow,
        service = "ECG API"
    })).WithTags("System");

    // ─────────────────────────────────────────────────────────────────────────
    // Fund endpoints
    // ─────────────────────────────────────────────────────────────────────────
    var funds = app.MapGroup("/api/funds").WithTags("Funds");

    // GET /api/funds/{fundName}/exposure?riskType=REGULATORY_WATCH&maxHops=4&asOfDate=2024-01-01
    funds.MapGet("/{fundName}/exposure", async (
        string fundName,
        ExposureQueryService svc,
        [FromQuery] string riskType = "REGULATORY_WATCH",
        [FromQuery] int maxHops = 4,
        [FromQuery] string? asOfDate = null) =>
    {
        var results = await svc.GetFundExposureChainAsync(
            Uri.UnescapeDataString(fundName), riskType, maxHops, asOfDate);

        return Results.Ok(new
        {
            fund = Uri.UnescapeDataString(fundName),
            riskType,
            maxHops,
            asOfDate = asOfDate ?? DateTime.UtcNow.ToString("yyyy-MM-dd"),
            exposureCount = results.Count,
            exposureChains = results
        });
    })
    .WithName("GetFundExposure")
    .WithSummary("Multi-hop indirect exposure analysis for a fund")
    .WithDescription("""
        Traverses Fund → Instrument → Issuer → (SUBSIDIARY_OF*1..N) → Risk-bearing Entity ← Risk.
        Returns all exposure paths up to maxHops ownership hops deep.
        Uses bi-temporal filtering — only follows ownership relationships active on asOfDate.
        """)
    .Produces<object>()
    .ProducesProblem(404);

    // GET /api/funds/{fundName}/portfolio?asOfDate=2024-01-01
    funds.MapGet("/{fundName}/portfolio", async (
        string fundName,
        ExposureQueryService svc,
        [FromQuery] string? asOfDate = null) =>
    {
        var positions = await svc.GetFundPortfolioAsync(
            Uri.UnescapeDataString(fundName), asOfDate);

        if (positions.Count == 0)
            return Results.NotFound(new { message = $"Fund '{fundName}' not found or has no positions." });

        var highRiskCount = positions.Count(p => p.WorstRiskSeverity == "HIGH");
        var medRiskCount = positions.Count(p => p.WorstRiskSeverity == "MEDIUM");
        var totalValue = positions.Sum(p => p.CurrentValueMillions);

        return Results.Ok(new
        {
            fund = positions.First().Fund,
            fundType = positions.First().FundType,
            aumMillions = positions.First().FundAumMillions,
            asOfDate = asOfDate ?? DateTime.UtcNow.ToString("yyyy-MM-dd"),
            positionCount = positions.Count,
            totalPositionValueMillions = totalValue,
            riskSummary = new { highRisk = highRiskCount, mediumRisk = medRiskCount },
            positions
        });
    })
    .WithName("GetFundPortfolio")
    .WithSummary("Full portfolio view with risk overlay")
    .Produces<object>()
    .ProducesProblem(404);

    // ─────────────────────────────────────────────────────────────────────────
    // Company endpoints
    // ─────────────────────────────────────────────────────────────────────────
    var companies = app.MapGroup("/api/companies").WithTags("Companies");

    // GET /api/companies/{name}/ownership-chain?asOfDate=2024-01-01
    companies.MapGet("/{name}/ownership-chain", async (
        string name,
        ExposureQueryService svc,
        [FromQuery] string? asOfDate = null) =>
    {
        var chain = await svc.GetOwnershipChainAsync(
            Uri.UnescapeDataString(name), asOfDate);

        if (chain is null)
            return Results.NotFound(new
            {
                message = $"No ownership chain found for '{name}'. " +
                          "The company may be an ultimate parent or not exist in the graph."
            });

        return Results.Ok(chain);
    })
    .WithName("GetOwnershipChain")
    .WithSummary("Trace full corporate ownership chain to ultimate beneficial owner")
    .Produces<OwnershipChainResult>()
    .ProducesProblem(404);

    // ─────────────────────────────────────────────────────────────────────────
    // Risk endpoints
    // ─────────────────────────────────────────────────────────────────────────
    var risks = app.MapGroup("/api/risks").WithTags("Risks");

    // GET /api/risks/{riskId}/propagation?asOfDate=2024-01-01
    risks.MapGet("/{riskId}/propagation", async (
        string riskId,
        ExposureQueryService svc,
        [FromQuery] string? asOfDate = null) =>
    {
        var results = await svc.GetRiskPropagationAsync(riskId, asOfDate);

        if (results.Count == 0)
            return Results.NotFound(new { message = $"Risk '{riskId}' not found or has no propagation paths." });

        return Results.Ok(new
        {
            riskId,
            affectedEntities = results.Count,
            affectedFunds = results.Select(r => r.AffectedFund).Where(f => f != null).Distinct().Count(),
            totalExposureMillions = results.Sum(r => r.CurrentPositionValue ?? 0),
            propagation = results
        });
    })
    .WithName("GetRiskPropagation")
    .WithSummary("Downstream blast radius for a risk flag")
    .Produces<object>()
    .ProducesProblem(404);

    // GET /api/risks/exposure?riskType=REGULATORY_WATCH
    risks.MapGet("/exposure", async (
        ExposureQueryService svc,
        [FromQuery] string riskType = "REGULATORY_WATCH",
        [FromQuery] string? asOfDate = null) =>
    {
        var results = await svc.FindIndirectExposureByRiskTypeAsync(riskType, asOfDate);
        return Results.Ok(new
        {
            riskType,
            totalExposurePaths = results.Count,
            affectedFunds = results.Select(r => r.Fund).Distinct().Count(),
            exposureChains = results
        });
    })
    .WithName("GetPortfolioExposureByRiskType")
    .WithSummary("All indirect exposure paths for a risk type across the entire portfolio")
    .Produces<object>();

    // ─────────────────────────────────────────────────────────────────────────
    // Graph admin endpoints
    // ─────────────────────────────────────────────────────────────────────────
    var graph = app.MapGroup("/api/graph").WithTags("Graph");

    // GET /api/graph/stats
    graph.MapGet("/stats", async (GraphRepository repo) =>
    {
        var counts = await repo.GetGraphStatsAsync();
        return Results.Ok(new GraphStats
        {
            TotalNodes = counts.GetValueOrDefault("nodes"),
            TotalRelationships = counts.GetValueOrDefault("relationships"),
            Companies = counts.GetValueOrDefault("companies"),
            Funds = counts.GetValueOrDefault("funds"),
            Deals = counts.GetValueOrDefault("deals"),
            Instruments = counts.GetValueOrDefault("instruments"),
            Risks = counts.GetValueOrDefault("risks")
        });
    })
    .WithName("GetGraphStats")
    .WithSummary("Node and relationship counts by label")
    .Produces<GraphStats>();

    // ─────────────────────────────────────────────────────────────────────────
    // Global error handling
    // ─────────────────────────────────────────────────────────────────────────
    app.Use(async (context, next) =>
    {
        try
        {
            await next(context);
        }
        catch (GraphNodeNotFoundException ex)
        {
            Log.Warning("Graph node not found: {Label} {Id}", ex.NodeLabel, ex.NodeId);
            context.Response.StatusCode = 404;
            await context.Response.WriteAsJsonAsync(new { error = ex.Message });
        }
        catch (TraversalTimeoutException ex)
        {
            Log.Error("Traversal timeout: {Query} elapsed={Elapsed}", ex.QueryName, ex.Elapsed);
            context.Response.StatusCode = 504;
            await context.Response.WriteAsJsonAsync(new { error = ex.Message });
        }
    });

    await app.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "ECG API terminated unexpectedly");
}
finally
{
    Log.CloseAndFlush();
}
