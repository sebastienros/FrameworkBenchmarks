FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /app
COPY PlatformBenchmarks .
RUN dotnet publish -c Release -o out /p:DatabaseProvider=MySqlConnector

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS runtime
ENV ASPNETCORE_URLS http://+:8080
WORKDIR /app
COPY --from=build /app/out ./
COPY PlatformBenchmarks/appsettings.mysql.json ./appsettings.json

EXPOSE 8080

ENTRYPOINT ["dotnet", "PlatformBenchmarks.dll"]
