// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient;

/// <summary>
/// Options for configuring a PostgreSQL connection pool.
/// </summary>
public sealed class PgPoolOptions
{
    /// <summary>
    /// Default maximum pool size.
    /// </summary>
    public const int DefaultMaxSize = 4;

    /// <summary>
    /// Default maximum wait queue size (0 = unbounded).
    /// </summary>
    public const int DefaultMaxWaitQueueSize = 0;

    /// <summary>
    /// Default idle timeout in milliseconds (0 = no timeout).
    /// </summary>
    public const int DefaultIdleTimeout = 0;

    /// <summary>
    /// Default connection timeout in milliseconds (30 seconds).
    /// </summary>
    public const int DefaultConnectionTimeout = 30000;

    /// <summary>
    /// Default maximum lifetime in milliseconds (0 = no limit).
    /// </summary>
    public const int DefaultMaxLifetime = 0;

    private int _maxSize = DefaultMaxSize;

    /// <summary>
    /// Gets or sets the maximum pool size.
    /// </summary>
    public int MaxSize
    {
        get => _maxSize;
        set
        {
            if (value < 1)
                throw new ArgumentException("Max pool size must be at least 1", nameof(value));
            _maxSize = value;
        }
    }

    /// <summary>
    /// Gets or sets the maximum wait queue size.
    /// When the pool is exhausted, requests are queued up to this limit.
    /// 0 means unbounded.
    /// </summary>
    public int MaxWaitQueueSize { get; set; } = DefaultMaxWaitQueueSize;

    /// <summary>
    /// Gets or sets the idle timeout in milliseconds.
    /// Connections idle longer than this are closed.
    /// 0 means no timeout.
    /// </summary>
    public int IdleTimeout { get; set; } = DefaultIdleTimeout;

    /// <summary>
    /// Gets or sets the connection timeout in milliseconds.
    /// Maximum time to wait for a connection from the pool.
    /// </summary>
    public int ConnectionTimeout { get; set; } = DefaultConnectionTimeout;

    /// <summary>
    /// Gets or sets the maximum lifetime of a connection in milliseconds.
    /// Connections older than this are closed.
    /// 0 means no limit.
    /// </summary>
    public int MaxLifetime { get; set; } = DefaultMaxLifetime;

    /// <summary>
    /// Gets or sets the pool name for identification/metrics.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Gets or sets whether to use pipelined mode.
    /// When true, multiple requests can be multiplexed on a single connection.
    /// </summary>
    public bool Pipelined { get; set; } = true;

    public PgPoolOptions() { }

    public PgPoolOptions(PgPoolOptions other)
    {
        _maxSize = other._maxSize;
        MaxWaitQueueSize = other.MaxWaitQueueSize;
        IdleTimeout = other.IdleTimeout;
        ConnectionTimeout = other.ConnectionTimeout;
        MaxLifetime = other.MaxLifetime;
        Name = other.Name;
        Pipelined = other.Pipelined;
    }

    public PgPoolOptions SetMaxSize(int maxSize) { MaxSize = maxSize; return this; }
    public PgPoolOptions SetMaxWaitQueueSize(int size) { MaxWaitQueueSize = size; return this; }
    public PgPoolOptions SetIdleTimeout(int timeout) { IdleTimeout = timeout; return this; }
    public PgPoolOptions SetConnectionTimeout(int timeout) { ConnectionTimeout = timeout; return this; }
    public PgPoolOptions SetMaxLifetime(int lifetime) { MaxLifetime = lifetime; return this; }
    public PgPoolOptions SetName(string? name) { Name = name; return this; }
    public PgPoolOptions SetPipelined(bool pipelined) { Pipelined = pipelined; return this; }
}
