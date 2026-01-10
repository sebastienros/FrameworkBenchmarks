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

    /// <summary>
    /// Creates a PgPoolOptions from a connection URI string.
    /// Supported parameters:
    /// - pool_size or poolsize: Maximum pool size
    /// - pool_name or poolname: Pool name for identification
    /// - pipelined: Whether to use pipelined mode (true/false)
    /// - idle_timeout or idletimeout: Idle timeout in milliseconds
    /// - connection_timeout or connectiontimeout: Connection timeout in milliseconds
    /// - max_lifetime or maxlifetime: Maximum connection lifetime in milliseconds
    /// - max_wait_queue_size or maxwaitqueuesize: Maximum wait queue size
    /// </summary>
    public static PgPoolOptions FromUri(string connectionUri)
    {
        var options = new PgPoolOptions();
        
        // Find the query string part
        int queryIndex = connectionUri.IndexOf('?');
        if (queryIndex < 0)
        {
            return options;
        }
        
        string queryString = connectionUri[(queryIndex + 1)..];
        ParseParameters(queryString, options);
        
        return options;
    }

    private static void ParseParameters(string parametersInfo, PgPoolOptions options)
    {
        if (string.IsNullOrEmpty(parametersInfo))
        {
            return;
        }

        ReadOnlySpan<char> span = parametersInfo.AsSpan();
        
        foreach (var range in span.Split('&'))
        {
            var parameterPair = span[range];
            
            if (parameterPair.IsEmpty)
            {
                continue;
            }

            int indexOfDelimiter = parameterPair.IndexOf('=');
            if (indexOfDelimiter < 0)
            {
                continue; // Skip malformed parameters
            }
            
            var key = parameterPair[..indexOfDelimiter].ToString().ToLowerInvariant();
            var value = Uri.UnescapeDataString(parameterPair[(indexOfDelimiter + 1)..].Trim().ToString());

            switch (key)
            {
                case "pool_size":
                case "poolsize":
                    if (int.TryParse(value, out var poolSize) && poolSize >= 1)
                    {
                        options.MaxSize = poolSize;
                    }
                    break;
                    
                case "pool_name":
                case "poolname":
                    options.Name = value;
                    break;
                    
                case "pipelined":
                    if (bool.TryParse(value, out var pipelined))
                    {
                        options.Pipelined = pipelined;
                    }
                    else if (value == "1" || value.Equals("yes", StringComparison.OrdinalIgnoreCase))
                    {
                        options.Pipelined = true;
                    }
                    else if (value == "0" || value.Equals("no", StringComparison.OrdinalIgnoreCase))
                    {
                        options.Pipelined = false;
                    }
                    break;
                    
                case "idle_timeout":
                case "idletimeout":
                    if (int.TryParse(value, out var idleTimeout) && idleTimeout >= 0)
                    {
                        options.IdleTimeout = idleTimeout;
                    }
                    break;
                    
                case "connection_timeout":
                case "connectiontimeout":
                    if (int.TryParse(value, out var connectionTimeout) && connectionTimeout >= 0)
                    {
                        options.ConnectionTimeout = connectionTimeout;
                    }
                    break;
                    
                case "max_lifetime":
                case "maxlifetime":
                    if (int.TryParse(value, out var maxLifetime) && maxLifetime >= 0)
                    {
                        options.MaxLifetime = maxLifetime;
                    }
                    break;
                    
                case "max_wait_queue_size":
                case "maxwaitqueuesize":
                    if (int.TryParse(value, out var maxWaitQueueSize) && maxWaitQueueSize >= 0)
                    {
                        options.MaxWaitQueueSize = maxWaitQueueSize;
                    }
                    break;
            }
        }
    }

    public PgPoolOptions SetMaxSize(int maxSize) { MaxSize = maxSize; return this; }
    public PgPoolOptions SetMaxWaitQueueSize(int size) { MaxWaitQueueSize = size; return this; }
    public PgPoolOptions SetIdleTimeout(int timeout) { IdleTimeout = timeout; return this; }
    public PgPoolOptions SetConnectionTimeout(int timeout) { ConnectionTimeout = timeout; return this; }
    public PgPoolOptions SetMaxLifetime(int lifetime) { MaxLifetime = lifetime; return this; }
    public PgPoolOptions SetName(string? name) { Name = name; return this; }
    public PgPoolOptions SetPipelined(bool pipelined) { Pipelined = pipelined; return this; }
}
