#!/usr/bin/env pwsh
# ECG Platform — .NET Solution Initializer
# Run once after cloning the repo to create the solution file and project references.
# Usage: pwsh setup.ps1

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "============================================================"
Write-Host " ECG Platform — .NET Solution Setup"
Write-Host "============================================================"
Write-Host ""

# Check dotnet is available
if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) {
    Write-Error ".NET 8 SDK not found. Install from https://dotnet.microsoft.com/download"
    exit 1
}

$dotnetVersion = dotnet --version
Write-Host "Using .NET SDK: $dotnetVersion"

# Remove stale solution file if present
if (Test-Path "ECG-Platform.sln") {
    Write-Host "Removing existing ECG-Platform.sln..."
    Remove-Item "ECG-Platform.sln"
}

# Create solution
Write-Host "Creating solution..."
dotnet new sln -n ECG-Platform

# Add all projects
Write-Host "Adding projects to solution..."
$projects = @(
    "src/ECG.Core/ECG.Core.csproj",
    "src/ECG.Graph/ECG.Graph.csproj",
    "src/ECG.Api/ECG.Api.csproj",
    "src/ECG.Streaming/ECG.Streaming.csproj",
    "src/ECG.Federation/ECG.Federation.csproj",
    "tests/ECG.Graph.Tests/ECG.Graph.Tests.csproj"
)

foreach ($proj in $projects) {
    if (Test-Path $proj) {
        dotnet sln add $proj
        Write-Host "  + $proj"
    } else {
        Write-Warning "  ? $proj not found — skipping"
    }
}

Write-Host ""
Write-Host "Restoring NuGet packages..."
dotnet restore ECG-Platform.sln

Write-Host ""
Write-Host "Building solution..."
dotnet build ECG-Platform.sln -c Debug --no-restore

Write-Host ""
Write-Host "============================================================"
Write-Host " Setup complete!"
Write-Host ""
Write-Host " Next steps:"
Write-Host "   1. docker-compose up -d"
Write-Host "   2. docker exec -i ecg-neo4j cypher-shell -u neo4j -p ecg_password123 < queries/schema_setup.cypher"
Write-Host "   3. python scripts/seed_graph.py"
Write-Host "   4. cd src/ECG.Api && dotnet run"
Write-Host "   5. curl http://localhost:5000/api/graph/stats"
Write-Host "============================================================"
Write-Host ""
