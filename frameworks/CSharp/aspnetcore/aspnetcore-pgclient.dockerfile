FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /app
COPY src/Platform .
COPY src/Vertx.PgClient /Vertx.PgClient
RUN dotnet publish Platform.csproj -c Release -o out /p:DatabaseProvider=PgClient

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
ENV URLS=http://+:8080
ENV DOTNET_GCDynamicAdaptationMode=0
ENV DOTNET_ReadyToRun=0
ENV DOTNET_HillClimbing_Disable=1

WORKDIR /app
COPY --from=build /app/out ./
COPY appsettings.pgclient.json ./appsettings.json

EXPOSE 8080

ENTRYPOINT ["dotnet", "Platform.dll"]
