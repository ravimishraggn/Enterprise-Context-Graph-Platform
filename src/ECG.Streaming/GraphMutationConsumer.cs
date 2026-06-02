using Confluent.Kafka;
using ECG.Core.Events;
using ECG.Core.Exceptions;
using ECG.Graph;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace ECG.Streaming;

/// <summary>
/// Kafka consumer that subscribes to all ECG event topics and applies
/// graph mutations (node creation, relationship updates, bi-temporal ownership changes).
///
/// PARTITION KEY STRATEGY:
///   ecg.deal.*          — keyed by DealId
///   ecg.ownership.*     — keyed by ChildCompanyId (canonical entity ID)
///   ecg.risk.*          — keyed by AttachedToEntityId
///   ecg.valuation.*     — keyed by InstrumentId
///
/// WHY ENTITY ID NOT EVENT ID: All events affecting the same entity land on
/// the same partition, guaranteeing causal ordering per entity. Two concurrent
/// ownership restructurings for different subsidiaries are independent and
/// safely processed in parallel across partitions.
/// </summary>
public sealed class GraphMutationConsumer : BackgroundService
{
    private static readonly string[] Topics =
    [
        "ecg.deal.created",
        "ecg.deal.status.changed",
        "ecg.ownership.changed",
        "ecg.risk.flagged",
        "ecg.valuation.updated"
    ];

    private readonly GraphRepository _repo;
    private readonly IConfiguration _config;
    private readonly ILogger<GraphMutationConsumer> _logger;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public GraphMutationConsumer(
        GraphRepository repo,
        IConfiguration config,
        ILogger<GraphMutationConsumer> logger)
    {
        _repo = repo;
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:9092",
            GroupId = _config["Kafka:ConsumerGroupId"] ?? "ecg-graph-mutator",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,    // Manual commit after successful graph write
            SessionTimeoutMs = 30_000,
            MaxPollIntervalMs = 300_000
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, e) =>
                _logger.LogError("Kafka consumer error: {Reason} — IsFatal={Fatal}", e.Reason, e.IsFatal))
            .SetPartitionsAssignedHandler((_, partitions) =>
                _logger.LogInformation("Partitions assigned: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]"))))
            .Build();

        consumer.Subscribe(Topics);
        _logger.LogInformation("Graph mutation consumer started, subscribed to: {Topics}",
            string.Join(", ", Topics));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var msg = consumer.Consume(TimeSpan.FromSeconds(1));
                if (msg is null) continue;

                _logger.LogDebug("Consumed: topic={Topic} partition={Partition} offset={Offset} key={Key}",
                    msg.Topic, msg.Partition, msg.Offset, msg.Message.Key);

                var handled = await HandleMessageAsync(msg.Topic, msg.Message.Key, msg.Message.Value);

                if (handled)
                {
                    consumer.Commit(msg);
                }
                else
                {
                    _logger.LogWarning("Message not handled — skipping commit: topic={Topic} offset={Offset}",
                        msg.Topic, msg.Offset);
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in consumer loop — continuing");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }

        consumer.Close();
        _logger.LogInformation("Graph mutation consumer stopped.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Event routing
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<bool> HandleMessageAsync(string topic, string key, string payload)
    {
        try
        {
            return topic switch
            {
                "ecg.deal.created"         => await HandleDealCreatedAsync(payload),
                "ecg.deal.status.changed"  => await HandleDealStatusChangedAsync(payload),
                "ecg.ownership.changed"    => await HandleOwnershipChangedAsync(payload),
                "ecg.risk.flagged"         => await HandleRiskFlaggedAsync(payload),
                "ecg.valuation.updated"    => await HandleValuationUpdatedAsync(payload),
                _ => throw new InvalidOperationException($"Unknown topic: {topic}")
            };
        }
        catch (GraphMutationException ex)
        {
            // Log and skip — this event cannot be applied (entity missing, etc.)
            _logger.LogError("Graph mutation failed: eventId={EventId} type={EventType} — {Message}",
                ex.EventId, ex.EventType, ex.Message);
            // Return true to commit and move past the poison message
            return true;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ecg.deal.created
    // ─────────────────────────────────────────────────────────────────────────
    private async Task<bool> HandleDealCreatedAsync(string payload)
    {
        var evt = JsonSerializer.Deserialize<DealCreatedEvent>(payload, JsonOpts)
            ?? throw new JsonException("Failed to deserialize DealCreatedEvent");

        _logger.LogInformation("Processing DealCreated: dealId={DealId} name={Name}",
            evt.DealId, evt.DealName);

        // Upsert the Deal node
        var dealProps = new Dictionary<string, object?>
        {
            ["id"] = evt.DealId,
            ["dealName"] = evt.DealName,
            ["dealType"] = evt.DealType,
            ["dealSizeMillions"] = evt.DealSizeMillions,
            ["currency"] = evt.Currency,
            ["status"] = evt.Status,
            ["closeDate"] = evt.CloseDate?.ToString("o"),
            ["sourceSystem"] = evt.SourceSystem,
            ["createdAt"] = evt.Timestamp.ToString("o"),
            ["updatedAt"] = evt.Timestamp.ToString("o")
        };

        await ExecuteMergeAsync("Deal", evt.DealId, dealProps);

        // INVOLVES relationship: Deal → Borrower (BORROWER role)
        if (!string.IsNullOrWhiteSpace(evt.BorrowerCompanyId))
        {
            await _repo.CreateRelationshipAsync(evt.DealId, evt.BorrowerCompanyId, "INVOLVES",
                new Dictionary<string, object?>
                {
                    ["role"] = "BORROWER",
                    ["effectiveDate"] = evt.Timestamp.ToString("o"),
                    ["sourceSystem"] = evt.SourceSystem
                });
        }

        // INVOLVES relationship: Deal → Sponsor (SPONSOR role)
        if (!string.IsNullOrWhiteSpace(evt.SponsorCompanyId))
        {
            await _repo.CreateRelationshipAsync(evt.DealId, evt.SponsorCompanyId, "INVOLVES",
                new Dictionary<string, object?>
                {
                    ["role"] = "SPONSOR",
                    ["effectiveDate"] = evt.Timestamp.ToString("o"),
                    ["sourceSystem"] = evt.SourceSystem
                });
        }

        // INVESTED_IN: Fund → Deal for each participating fund
        foreach (var fundId in evt.FundIds)
        {
            await _repo.CreateRelationshipAsync(fundId, evt.DealId, "INVESTED_IN",
                new Dictionary<string, object?>
                {
                    ["committedMillions"] = 0m,
                    ["calledMillions"] = 0m,
                    ["effectiveDate"] = evt.Timestamp.ToString("o"),
                    ["sourceSystem"] = evt.SourceSystem
                });
        }

        _logger.LogInformation("DealCreated applied: dealId={DealId}", evt.DealId);
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ecg.deal.status.changed
    // ─────────────────────────────────────────────────────────────────────────
    private async Task<bool> HandleDealStatusChangedAsync(string payload)
    {
        var evt = JsonSerializer.Deserialize<DealStatusChangedEvent>(payload, JsonOpts)
            ?? throw new JsonException("Failed to deserialize DealStatusChangedEvent");

        _logger.LogInformation("Processing DealStatusChanged: dealId={DealId} {Old} → {New}",
            evt.DealId, evt.OldStatus, evt.NewStatus);

        const string cypher = """
            MATCH (d:Deal {id: $dealId})
            SET d.status = $newStatus, d.updatedAt = datetime()
            """;

        await _repo.ExecuteRawReadAsync(cypher, new { dealId = evt.DealId, newStatus = evt.NewStatus });
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ecg.ownership.changed — BI-TEMPORAL OWNERSHIP UPDATE
    //
    // Algorithm:
    //   1. Find the currently active SUBSIDIARY_OF relationship (expiryDate IS NULL)
    //   2. Set expiryDate = event.EffectiveDate on the old relationship
    //   3. Create a new SUBSIDIARY_OF with effectiveDate = event.EffectiveDate
    //   Never DELETE — always expire + create.
    // ─────────────────────────────────────────────────────────────────────────
    private async Task<bool> HandleOwnershipChangedAsync(string payload)
    {
        var evt = JsonSerializer.Deserialize<OwnershipChangedEvent>(payload, JsonOpts)
            ?? throw new JsonException("Failed to deserialize OwnershipChangedEvent");

        _logger.LogInformation(
            "Processing OwnershipChanged: child={Child} oldParent={OldParent} → newParent={NewParent} ({Pct}%)",
            evt.ChildCompanyId, evt.OldParentCompanyId, evt.NewParentCompanyId, evt.NewOwnershipPct);

        await _repo.UpdateOwnershipAsync(
            childId: evt.ChildCompanyId,
            oldParentId: evt.OldParentCompanyId,
            newParentId: evt.NewParentCompanyId,
            ownershipPct: evt.NewOwnershipPct,
            ownershipType: evt.OwnershipType,
            effectiveDate: evt.EffectiveDate,
            sourceSystem: "KAFKA_EVENT");

        _logger.LogInformation("OwnershipChanged applied: child={Child}", evt.ChildCompanyId);
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ecg.risk.flagged
    // ─────────────────────────────────────────────────────────────────────────
    private async Task<bool> HandleRiskFlaggedAsync(string payload)
    {
        var evt = JsonSerializer.Deserialize<RiskFlaggedEvent>(payload, JsonOpts)
            ?? throw new JsonException("Failed to deserialize RiskFlaggedEvent");

        _logger.LogInformation("Processing RiskFlagged: riskId={RiskId} type={Type} severity={Sev} on {Entity}",
            evt.RiskId, evt.RiskType, evt.Severity, evt.AttachedToEntityId);

        // Upsert the Risk node
        var riskProps = new Dictionary<string, object?>
        {
            ["id"] = evt.RiskId,
            ["riskType"] = evt.RiskType,
            ["severity"] = evt.Severity,
            ["description"] = evt.Description,
            ["flaggedDate"] = evt.Timestamp.ToString("yyyy-MM-dd"),
            ["isActive"] = true,
            ["flaggedBy"] = evt.FlaggedBy,
            ["sourceSystem"] = "KAFKA_EVENT",
            ["createdAt"] = evt.Timestamp.ToString("o"),
            ["updatedAt"] = evt.Timestamp.ToString("o")
        };

        await ExecuteMergeAsync("Risk", evt.RiskId, riskProps);

        // ATTACHED_TO relationship: Risk → Entity
        await _repo.CreateRelationshipAsync(evt.RiskId, evt.AttachedToEntityId, "ATTACHED_TO",
            new Dictionary<string, object?>
            {
                ["attachmentReason"] = evt.RiskType,
                ["effectiveDate"] = evt.Timestamp.ToString("o"),
                ["sourceSystem"] = "KAFKA_EVENT"
            });

        _logger.LogInformation("RiskFlagged applied: riskId={RiskId}", evt.RiskId);
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ecg.valuation.updated
    // ─────────────────────────────────────────────────────────────────────────
    private async Task<bool> HandleValuationUpdatedAsync(string payload)
    {
        var evt = JsonSerializer.Deserialize<ValuationUpdatedEvent>(payload, JsonOpts)
            ?? throw new JsonException("Failed to deserialize ValuationUpdatedEvent");

        _logger.LogInformation("Processing ValuationUpdated: instrument={Instrument} fund={Fund} value={Value}M",
            evt.InstrumentId, evt.FundId, evt.NewValueMillions);

        const string cypher = """
            MATCH (f:Fund {id: $fundId})-[pos:HAS_POSITION]->(i:Instrument {id: $instrumentId})
            WHERE pos.asOfDate = $previousAsOfDate OR pos.asOfDate IS NULL
            SET pos.currentValueMillions = $newValue,
                pos.weightPct            = $newWeightPct,
                pos.asOfDate             = $asOfDate,
                pos.updatedAt            = datetime()
            """;

        await _repo.ExecuteRawReadAsync(cypher, new
        {
            fundId = evt.FundId,
            instrumentId = evt.InstrumentId,
            newValue = (double)evt.NewValueMillions,
            newWeightPct = (double)evt.NewWeightPct,
            asOfDate = evt.AsOfDate,
            previousAsOfDate = evt.AsOfDate
        });

        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private async Task ExecuteMergeAsync(string label, string id, Dictionary<string, object?> props)
    {
        // label = typeof(T).Name — never user input. Use $$""" so {id: $id} = literal Cypher map.
        var cypher = $$"""
            MERGE (n:{{label}} {id: $id})
            SET n += $props
            SET n.updatedAt = datetime()
            """;

        await _repo.ExecuteRawReadAsync(cypher, new { id, props });
    }
}
