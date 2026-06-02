# Multi-stage Dockerfile for ECG.Api
# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy project files first (layer caching for restore)
COPY ["src/ECG.Api/ECG.Api.csproj",         "src/ECG.Api/"]
COPY ["src/ECG.Core/ECG.Core.csproj",       "src/ECG.Core/"]
COPY ["src/ECG.Graph/ECG.Graph.csproj",     "src/ECG.Graph/"]

RUN dotnet restore "src/ECG.Api/ECG.Api.csproj"

# Copy full source and build
COPY . .
WORKDIR "/src/src/ECG.Api"
RUN dotnet build "ECG.Api.csproj" -c Release -o /app/build --no-restore

# Stage 2: Publish
FROM build AS publish
RUN dotnet publish "ECG.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false --no-restore

# Stage 3: Runtime
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app

# Create logs directory
RUN mkdir -p /app/logs

# Non-root user for security
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser /app
USER appuser

COPY --from=publish /app/publish .

EXPOSE 8080
ENTRYPOINT ["dotnet", "ECG.Api.dll"]
