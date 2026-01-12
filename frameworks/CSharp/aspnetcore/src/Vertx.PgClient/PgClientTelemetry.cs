// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Vertx.PgClient;

internal static class PgClientTelemetry
{
    public const string TelemetryName = "Vertx.PgClient";

    public static readonly ActivitySource ActivitySource = new(TelemetryName);
    public static readonly Meter Meter = new(TelemetryName);

    public static readonly Counter<long> QueriesExecuted = Meter.CreateCounter<long>(
        name: "pgclient.queries.executed",
        unit: "{query}",
        description: "Number of queries executed via PgPool.");

    public static readonly Counter<long> ConnectionsCreated = Meter.CreateCounter<long>(
        name: "pgclient.connections.created",
        unit: "{connection}",
        description: "Number of physical PostgreSQL connections created by PgPool.");

    public static readonly Counter<long> ConnectionsDisposed = Meter.CreateCounter<long>(
        name: "pgclient.connections.disposed",
        unit: "{connection}",
        description: "Number of physical PostgreSQL connections disposed by PgPool.");
}
