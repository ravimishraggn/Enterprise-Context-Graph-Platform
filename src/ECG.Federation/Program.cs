using ECG.Federation.Schema;
using ECG.Federation.Services;
using ECG.Graph;
using ECG.Graph.Services;
using Serilog;
using Serilog.Events;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog();

    // ─────────────────────────────────────────────────────────────────────────
    // Services
    // ─────────────────────────────────────────────────────────────────────────
    builder.Services.AddSingleton<GraphRepository>();
    builder.Services.AddScoped<ExposureQueryService>();
    builder.Services.AddScoped<FederationService>();

    // HTTP client for mock warehouse API
    builder.Services.AddHttpClient("WarehouseApi", client =>
    {
        var warehouseUrl = builder.Configuration["WarehouseApi:BaseUrl"] ?? "http://localhost:8090";
        client.BaseAddress = new Uri(warehouseUrl);
        client.Timeout = TimeSpan.FromSeconds(5);
    });

    // Hot Chocolate GraphQL
    builder.Services
        .AddGraphQLServer()
        .AddQueryType<Query>()
        .AddProjections()
        .AddFiltering()
        .AddSorting();

    // ─────────────────────────────────────────────────────────────────────────
    // Pipeline
    // ─────────────────────────────────────────────────────────────────────────
    var app = builder.Build();

    app.UseSerilogRequestLogging();

    app.MapGet("/health", () => Results.Ok(new { status = "healthy", service = "ECG Federation" }));

    // GraphQL endpoint at /graphql, Banana Cake Pop UI at /graphql (in dev)
    app.MapGraphQL();

    Log.Information("ECG Federation gateway starting on :5001");
    await app.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "ECG Federation gateway crashed");
}
finally
{
    Log.CloseAndFlush();
}
