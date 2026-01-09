// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient;

/// <summary>
/// A notification emitted by Postgres (via LISTEN/NOTIFY).
/// </summary>
public sealed class PgNotification
{
    /// <summary>
    /// Gets or sets the notification process id.
    /// </summary>
    public int ProcessId { get; set; }

    /// <summary>
    /// Gets or sets the notification channel value.
    /// </summary>
    public string? Channel { get; set; }

    /// <summary>
    /// Gets or sets the notification payload value.
    /// </summary>
    public string? Payload { get; set; }

    public PgNotification() { }

    public PgNotification(string? channel, int processId, string? payload)
    {
        Channel = channel;
        ProcessId = processId;
        Payload = payload;
    }

    public PgNotification SetProcessId(int processId)
    {
        ProcessId = processId;
        return this;
    }

    public PgNotification SetChannel(string? channel)
    {
        Channel = channel;
        return this;
    }

    public PgNotification SetPayload(string? payload)
    {
        Payload = payload;
        return this;
    }
}
