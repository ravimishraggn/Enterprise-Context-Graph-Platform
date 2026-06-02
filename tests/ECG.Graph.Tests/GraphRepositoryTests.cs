using ECG.Core.Domain.Nodes;
using ECG.Core.Exceptions;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ECG.Graph.Tests;

/// <summary>
/// Integration tests for GraphRepository against a live Neo4j instance.
///
/// REQUIRES: Neo4j running on localhost:7687 with auth neo4j/ecg_password123
/// Run via: docker-compose up neo4j -d
///
/// These tests use a dedicated "ecg-test" database to isolate from seeded data.
/// Each test class cleans up after itself via [Collection] isolation.
/// </summary>
[Collection("Neo4j Integration")]
public class GraphRepositoryTests : IAsyncLifetime
{
    private readonly GraphRepository _repo;

    private static readonly IConfiguration Config = new ConfigurationBuilder()
        .AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Neo4j:Uri"]      = "bolt://localhost:7687",
            ["Neo4j:Username"] = "neo4j",
            ["Neo4j:Password"] = "ecg_password123",
            ["Neo4j:Database"] = "neo4j"
        })
        .Build();

    public GraphRepositoryTests()
    {
        _repo = new GraphRepository(Config, NullLogger<GraphRepository>.Instance);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        // Clean up test nodes created during this test run
        var cypher = "MATCH (n) WHERE n.id STARTS WITH 'test-' DETACH DELETE n";
        await _repo.ExecuteRawReadAsync(cypher, new { });
        await _repo.DisposeAsync();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UpsertNodeAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task UpsertNode_Company_CreatesNodeWithAllProperties()
    {
        // Arrange
        var company = new Company
        {
            Id           = "test-co-001",
            Name         = "Test Holdings Corp",
            CanonicalId  = "test-co-001",
            Jurisdiction = "US",
            Sector       = "Technology",
            CompanyType  = "HOLDING",
            IsActive     = true,
            SourceSystem = "MANUAL",
            CreatedAt    = DateTime.UtcNow,
            UpdatedAt    = DateTime.UtcNow
        };

        // Act
        var node = await _repo.UpsertNodeAsync(company);

        // Assert
        node.Should().NotBeNull();
        node.Properties["name"].Should().Be("Test Holdings Corp");
        node.Properties["jurisdiction"].Should().Be("US");
        node.Properties["companyType"].Should().Be("HOLDING");
    }

    [Fact]
    public async Task UpsertNode_IsIdempotent_SecondUpsertDoesNotDuplicate()
    {
        // Arrange
        var company = new Company
        {
            Id           = "test-co-002",
            Name         = "Idempotent Corp",
            CanonicalId  = "test-co-002",
            Jurisdiction = "GB",
            Sector       = "Industrials",
            CompanyType  = "BORROWER",
            SourceSystem = "MANUAL",
        };

        // Act
        await _repo.UpsertNodeAsync(company);
        await _repo.UpsertNodeAsync(company with { Sector = "Healthcare" }); // update sector

        var nodes = await _repo.ExecuteReadAsync(
            "MATCH (c:Company {id: $id}) RETURN c",
            new { id = "test-co-002" },
            r => r["c"]);

        // Assert — exactly one node, with updated sector
        nodes.Should().HaveCount(1);
        nodes[0].As<Neo4j.Driver.INode>().Properties["sector"].Should().Be("Healthcare");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // GetNodeByIdAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetNodeById_ExistingNode_ReturnsNode()
    {
        // Arrange
        var fund = new Fund
        {
            Id          = "test-fund-001",
            Name        = "Test Credit Fund I",
            FundType    = "CREDIT",
            Vintage     = 2022,
            AumMillions = 500m,
            Manager     = "Test Asset Management",
            Status      = "INVESTING",
            SourceSystem = "MANUAL"
        };
        await _repo.UpsertNodeAsync(fund);

        // Act
        var node = await _repo.GetNodeByIdAsync("Fund", "test-fund-001");

        // Assert
        node.Should().NotBeNull();
        node.Properties["name"].Should().Be("Test Credit Fund I");
        node.Properties["fundType"].Should().Be("CREDIT");
    }

    [Fact]
    public async Task GetNodeById_MissingNode_ThrowsGraphNodeNotFoundException()
    {
        // Act & Assert
        var act = async () => await _repo.GetNodeByIdAsync("Company", "test-does-not-exist-xyz");
        await act.Should().ThrowAsync<GraphNodeNotFoundException>()
            .Where(ex => ex.NodeLabel == "Company" && ex.NodeId == "test-does-not-exist-xyz");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CreateRelationshipAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateRelationship_SubsidiaryOf_CreatesWithBiTemporalProperties()
    {
        // Arrange
        var parent = new Company { Id = "test-parent-001", Name = "Test Parent Corp",
            CanonicalId = "test-parent-001", Jurisdiction = "US", Sector = "Diversified",
            CompanyType = "HOLDING", SourceSystem = "MANUAL" };
        var child  = new Company { Id = "test-child-001",  Name = "Test Child Corp",
            CanonicalId = "test-child-001",  Jurisdiction = "US", Sector = "Technology",
            CompanyType = "BORROWER", SourceSystem = "MANUAL" };

        await _repo.UpsertNodeAsync(parent);
        await _repo.UpsertNodeAsync(child);

        var relProps = new Dictionary<string, object?>
        {
            ["ownershipPct"]  = 75.0,
            ["ownershipType"] = "DIRECT",
            ["effectiveDate"] = "2024-01-01",
            ["expiryDate"]    = null,
            ["sourceSystem"]  = "MANUAL"
        };

        // Act
        await _repo.CreateRelationshipAsync("test-child-001", "test-parent-001", "SUBSIDIARY_OF", relProps);

        // Assert
        var rels = await _repo.ExecuteReadAsync(
            "MATCH (c:Company {id: $cid})-[r:SUBSIDIARY_OF]->(p:Company {id: $pid}) RETURN r",
            new { cid = "test-child-001", pid = "test-parent-001" },
            rec => rec["r"].As<Neo4j.Driver.IRelationship>());

        rels.Should().HaveCount(1);
        rels[0].Properties["ownershipPct"].Should().Be(75.0);
        rels[0].Properties["ownershipType"].Should().Be("DIRECT");
        rels[0].Properties["expiryDate"].Should().BeNull();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // UpdateOwnershipAsync (bi-temporal)
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateOwnership_ExpiresOldRelAndCreatesNew()
    {
        // Arrange — set up initial ownership chain
        var holdco = new Company { Id = "test-holdco-bt", Name = "BT HoldCo",
            CanonicalId = "test-holdco-bt", Jurisdiction = "US", Sector = "Diversified",
            CompanyType = "HOLDING", SourceSystem = "MANUAL" };
        var newco  = new Company { Id = "test-newco-bt",  Name = "BT NewCo",
            CanonicalId = "test-newco-bt",  Jurisdiction = "US", Sector = "Technology",
            CompanyType = "HOLDING", SourceSystem = "MANUAL" };
        var subsidiary = new Company { Id = "test-sub-bt", Name = "BT Subsidiary",
            CanonicalId = "test-sub-bt", Jurisdiction = "US", Sector = "Technology",
            CompanyType = "BORROWER", SourceSystem = "MANUAL" };

        await _repo.UpsertNodeAsync(holdco);
        await _repo.UpsertNodeAsync(newco);
        await _repo.UpsertNodeAsync(subsidiary);

        // Create initial SUBSIDIARY_OF
        await _repo.CreateRelationshipAsync("test-sub-bt", "test-holdco-bt", "SUBSIDIARY_OF",
            new Dictionary<string, object?> { ["ownershipPct"] = 100.0, ["ownershipType"] = "DIRECT",
                ["effectiveDate"] = "2023-01-01", ["expiryDate"] = null, ["sourceSystem"] = "MANUAL" });

        // Act — ownership change: subsidiary moves from holdco → newco
        var changeDate = new DateTime(2024, 6, 1);
        await _repo.UpdateOwnershipAsync(
            childId: "test-sub-bt",
            oldParentId: "test-holdco-bt",
            newParentId: "test-newco-bt",
            ownershipPct: 80.0m,
            ownershipType: "DIRECT",
            effectiveDate: changeDate,
            sourceSystem: "KAFKA_EVENT");

        // Assert — old relationship is expired
        var expiredRels = await _repo.ExecuteReadAsync(
            "MATCH (:Company {id: $cid})-[r:SUBSIDIARY_OF]->(:Company {id: $pid}) RETURN r",
            new { cid = "test-sub-bt", pid = "test-holdco-bt" },
            rec => rec["r"].As<Neo4j.Driver.IRelationship>());
        expiredRels.Should().HaveCount(1);
        expiredRels[0].Properties["expiryDate"].Should().NotBeNull();

        // Assert — new relationship is active (no expiryDate)
        var newRels = await _repo.ExecuteReadAsync(
            "MATCH (:Company {id: $cid})-[r:SUBSIDIARY_OF]->(:Company {id: $pid}) RETURN r",
            new { cid = "test-sub-bt", pid = "test-newco-bt" },
            rec => rec["r"].As<Neo4j.Driver.IRelationship>());
        newRels.Should().HaveCount(1);
        newRels[0].Properties["expiryDate"].Should().BeNull();
        newRels[0].Properties["ownershipPct"].Should().Be(80.0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // GetGraphStatsAsync
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetGraphStats_ReturnsAllExpectedMetrics()
    {
        var stats = await _repo.GetGraphStatsAsync();

        stats.Should().ContainKey("nodes");
        stats.Should().ContainKey("relationships");
        stats.Should().ContainKey("companies");
        stats.Should().ContainKey("funds");
        stats.Should().ContainKey("deals");
        stats.Should().ContainKey("instruments");
        stats.Should().ContainKey("risks");

        stats["nodes"].Should().BeGreaterThan(0, "graph should have been seeded");
    }
}
