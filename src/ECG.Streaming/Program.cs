using ECG.Graph;
using ECG.Streaming;
using Serilog;
using Serilog.Events;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate:
        "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}")
    .WriteTo.File("logs/ecg-streaming.log", rollingInterval: RollingInterval.Day, retainedFileCountLimit: 7)
    .CreateLogger();

try
{
    Log.Information("ECG Streaming consumer starting...");

    var host = Host.CreateDefaultBuilder(args)
        .UseSerilog()
        .ConfigureServices((ctx, services) =>
        {
            services.AddSingleton<GraphRepository>();
            services.AddHostedService<GraphMutationConsumer>();
        })
        .Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "ECG Streaming consumer crashed");
}
finally
{
    Log.CloseAndFlush();
}
