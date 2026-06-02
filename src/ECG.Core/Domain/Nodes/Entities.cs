namespace ECG.Core.Domain.Nodes;

/// <summary>
/// A legal entity — borrower, issuer, sponsor, counterparty, or holding company.
/// SUBSIDIARY_OF relationships between Company nodes form the ownership graph.
/// </summary>
public record Company : GraphNode
{
    public required string Name { get; init; }

    /// <summary>Master entity ID after cross-system entity resolution.</summary>
    public required string CanonicalId { get; init; }

    /// <summary>ISO 3166-1 alpha-2 country code: US, GB, DE, FR, …</summary>
    public required string Jurisdiction { get; init; }

    /// <summary>GICS macro sector: Technology, Healthcare, Energy, …</summary>
    public required string Sector { get; init; }

    /// <summary>BORROWER | ISSUER | SPONSOR | COUNTERPARTY | HOLDING</summary>
    public required string CompanyType { get; init; }

    public bool IsActive { get; init; } = true;
}

/// <summary>
/// An investment fund — credit, private equity, or hybrid.
/// Funds hold positions in instruments via HAS_POSITION relationships.
/// </summary>
public record Fund : GraphNode
{
    public required string Name { get; init; }

    /// <summary>CREDIT | PE | HYBRID | MEZZ | INFRASTRUCTURE | REAL_ESTATE</summary>
    public required string FundType { get; init; }

    public int Vintage { get; init; }
    public decimal AumMillions { get; init; }
    public required string Manager { get; init; }

    /// <summary>INVESTING | HARVESTING | CLOSED | WIND_DOWN</summary>
    public required string Status { get; init; }
}

/// <summary>
/// A capital markets transaction — LBO, credit facility, growth equity, etc.
/// Connects sponsors, borrowers, and instruments in a single event context.
/// </summary>
public record Deal : GraphNode
{
    public required string DealName { get; init; }

    /// <summary>LBO | GROWTH_EQUITY | CREDIT_FACILITY | MEZZ | RECAPITALIZATION</summary>
    public required string DealType { get; init; }

    public DateTime? CloseDate { get; init; }
    public decimal DealSizeMillions { get; init; }

    /// <summary>PIPELINE | CLOSED | EXITED | WRITTEN_OFF | ON_HOLD</summary>
    public required string Status { get; init; }

    public required string Currency { get; init; }
}

/// <summary>
/// A specific debt or equity instrument held in a portfolio.
/// Linked to its issuing company via ISSUED_BY and to deals via FINANCED_BY.
/// </summary>
public record Instrument : GraphNode
{
    public string? Isin { get; init; }
    public string? Cusip { get; init; }

    /// <summary>TERM_LOAN_A | TERM_LOAN_B | REVOLVING_CREDIT | BOND | MEZZ_NOTE | EQUITY</summary>
    public required string InstrumentType { get; init; }

    public required string Name { get; init; }
    public decimal FaceValueMillions { get; init; }

    /// <summary>Annual coupon rate as a decimal (e.g. 0.085 = 8.5% SOFR + 300bps)</summary>
    public decimal? Coupon { get; init; }

    public DateTime? MaturityDate { get; init; }
    public required string Currency { get; init; }
}

/// <summary>
/// A limited partner or co-investor in a fund.
/// LP_IN relationships capture commitment size and type.
/// </summary>
public record Investor : GraphNode
{
    public required string Name { get; init; }

    /// <summary>LP | CO_INVESTOR | ANCHOR | SEEDER</summary>
    public required string InvestorType { get; init; }

    public required string Domicile { get; init; }
    public decimal CommittedCapitalMillions { get; init; }
}

/// <summary>
/// A risk flag attached to any entity in the graph.
/// Propagates through SUBSIDIARY_OF chains to expose indirect fund exposure.
/// </summary>
public record Risk : GraphNode
{
    /// <summary>CREDIT | REGULATORY_WATCH | OPERATIONAL | MARKET | LIQUIDITY</summary>
    public required string RiskType { get; init; }

    /// <summary>HIGH | MEDIUM | LOW</summary>
    public required string Severity { get; init; }

    public DateTime FlaggedDate { get; init; }
    public required string Description { get; init; }
    public bool IsActive { get; init; } = true;

    /// <summary>SYSTEM | ANALYST | REGULATORY_FEED | MARKET_DATA</summary>
    public required string FlaggedBy { get; init; }
}

/// <summary>
/// A document associated with a deal or company.
/// EmbeddingId links to the Qdrant vector store for semantic search.
/// </summary>
public record Document : GraphNode
{
    /// <summary>CIM | TERM_SHEET | ANNUAL_REPORT | COVENANT_REPORT | LEGAL_OPINION</summary>
    public required string DocType { get; init; }

    public required string Title { get; init; }

    /// <summary>Qdrant point UUID for semantic similarity lookup.</summary>
    public string? EmbeddingId { get; init; }

    /// <summary>Object storage path: s3://bucket/path or /mnt/docs/path</summary>
    public required string StoragePath { get; init; }

    public DateTime DocumentDate { get; init; }
}

/// <summary>
/// An individual — board member, executive, or sponsor representative.
/// SERVES_ON_BOARD_OF relationships enable board interlocking analysis.
/// </summary>
public record Person : GraphNode
{
    public required string FullName { get; init; }

    /// <summary>CEO | CFO | BOARD_MEMBER | SPONSOR_REP | MANAGING_DIRECTOR</summary>
    public required string Role { get; init; }

    public string? Email { get; init; }
}
