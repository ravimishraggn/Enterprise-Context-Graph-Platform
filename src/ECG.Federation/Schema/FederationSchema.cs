using ECG.Federation.Services;

namespace ECG.Federation.Schema;

// ─────────────────────────────────────────────────────────────────────────────
// Hot Chocolate GraphQL Schema — Code-First approach
// ─────────────────────────────────────────────────────────────────────────────

/// <summary>
/// GraphQL query root.
/// All resolvers fan out: graph query first → parallel enrichment from other sources.
/// </summary>
public sealed class Query
{
    /// <summary>
    /// Full intelligence view for a fund: positions, exposure chains,
    /// related documents (from vector store), and financial metrics (from data warehouse).
    /// </summary>
    [GraphQLDescription("Returns complete fund intelligence: portfolio positions + risk exposure + document context + financial metrics")]
    public async Task<FundIntelligenceResult> FundIntelligence(
        string fundName,
        string? asOfDate,
        [Service] FederationService svc) =>
        await svc.GetFundIntelligenceAsync(fundName, asOfDate);

    /// <summary>
    /// Full company context: ownership chain + financial metrics.
    /// </summary>
    [GraphQLDescription("Returns ownership chain and financial metrics for a company")]
    public async Task<CompanyContextResult> CompanyContext(
        string companyName,
        string? asOfDate,
        [Service] FederationService svc) =>
        await svc.GetCompanyContextAsync(companyName, asOfDate);
}
