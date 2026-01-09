// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient;

/// <summary>
/// PostgreSQL connection options.
/// </summary>
public sealed class PgConnectOptions
{
    public const string DefaultHost = "localhost";
    public const int DefaultPort = 5432;
    public const string DefaultDatabase = "db";
    public const string DefaultUser = "user";
    public const string DefaultPassword = "pass";
    public const int DefaultPipeliningLimit = 256;
    public const SslMode DefaultSslMode = SslMode.Disable;
    public const bool DefaultUseLayer7Proxy = false;

    public static readonly IReadOnlyDictionary<string, string> DefaultProperties = new Dictionary<string, string>
    {
        ["application_name"] = "vertx-pg-client",
        ["client_encoding"] = "utf8",
        ["DateStyle"] = "ISO",
        ["extra_float_digits"] = "2"
    };

    public string Host { get; set; } = DefaultHost;
    public int Port { get; set; } = DefaultPort;
    public string User { get; set; } = DefaultUser;
    public string Password { get; set; } = DefaultPassword;
    public string Database { get; set; } = DefaultDatabase;
    public SslMode SslMode { get; set; } = DefaultSslMode;
    public bool UseLayer7Proxy { get; set; } = DefaultUseLayer7Proxy;
    
    private int _pipeliningLimit = DefaultPipeliningLimit;
    public int PipeliningLimit
    {
        get => _pipeliningLimit;
        set
        {
            if (value < 1)
            {
                throw new ArgumentException("Pipelining limit must be at least 1", nameof(value));
            }
            _pipeliningLimit = value;
        }
    }

    public bool CachePreparedStatements { get; set; }
    public int PreparedStatementCacheMaxSize { get; set; } = 256;
    public int PreparedStatementCacheSqlLimit { get; set; } = 2048;
    public int ReconnectAttempts { get; set; }
    public TimeSpan ReconnectInterval { get; set; } = TimeSpan.FromSeconds(1);

    public Dictionary<string, string> Properties { get; set; } = new(DefaultProperties);

    /// <summary>
    /// SSL/TLS options for the connection.
    /// </summary>
    public PgSslOptions? SslOptions { get; set; }

    public PgConnectOptions() { }

    public PgConnectOptions(PgConnectOptions other)
    {
        Host = other.Host;
        Port = other.Port;
        User = other.User;
        Password = other.Password;
        Database = other.Database;
        SslMode = other.SslMode;
        UseLayer7Proxy = other.UseLayer7Proxy;
        _pipeliningLimit = other._pipeliningLimit;
        CachePreparedStatements = other.CachePreparedStatements;
        PreparedStatementCacheMaxSize = other.PreparedStatementCacheMaxSize;
        PreparedStatementCacheSqlLimit = other.PreparedStatementCacheSqlLimit;
        ReconnectAttempts = other.ReconnectAttempts;
        ReconnectInterval = other.ReconnectInterval;
        Properties = new Dictionary<string, string>(other.Properties);
        SslOptions = other.SslOptions is not null ? new PgSslOptions(other.SslOptions) : null;
    }

    /// <summary>
    /// Creates a PgConnectOptions configured from a connection URI.
    /// </summary>
    public static PgConnectOptions FromUri(string connectionUri)
    {
        return PgConnectionUriParser.Parse(connectionUri);
    }

    /// <summary>
    /// Creates a PgConnectOptions configured with environment variables.
    /// </summary>
    public static PgConnectOptions FromEnv()
    {
        var options = new PgConnectOptions();

        var pgHostAddr = Environment.GetEnvironmentVariable("PGHOSTADDR");
        var pgHost = Environment.GetEnvironmentVariable("PGHOST");

        if (pgHostAddr is null)
        {
            if (pgHost is not null)
            {
                options.Host = pgHost;
            }
        }
        else
        {
            options.Host = pgHostAddr;
        }

        var pgPort = Environment.GetEnvironmentVariable("PGPORT");
        if (pgPort is not null && int.TryParse(pgPort, out var port))
        {
            options.Port = port;
        }

        var pgDatabase = Environment.GetEnvironmentVariable("PGDATABASE");
        if (pgDatabase is not null)
        {
            options.Database = pgDatabase;
        }

        var pgUser = Environment.GetEnvironmentVariable("PGUSER");
        if (pgUser is not null)
        {
            options.User = pgUser;
        }

        var pgPassword = Environment.GetEnvironmentVariable("PGPASSWORD");
        if (pgPassword is not null)
        {
            options.Password = pgPassword;
        }

        var pgSslMode = Environment.GetEnvironmentVariable("PGSSLMODE");
        if (pgSslMode is not null)
        {
            options.SslMode = SslModeExtensions.Parse(pgSslMode);
        }

        return options;
    }

    public PgConnectOptions SetHost(string host) { Host = host; return this; }
    public PgConnectOptions SetPort(int port) { Port = port; return this; }
    public PgConnectOptions SetUser(string user) { User = user; return this; }
    public PgConnectOptions SetPassword(string password) { Password = password; return this; }
    public PgConnectOptions SetDatabase(string database) { Database = database; return this; }
    public PgConnectOptions SetSslMode(SslMode sslMode) { SslMode = sslMode; return this; }
    public PgConnectOptions SetUseLayer7Proxy(bool useLayer7Proxy) { UseLayer7Proxy = useLayer7Proxy; return this; }
    public PgConnectOptions SetPipeliningLimit(int pipeliningLimit) { PipeliningLimit = pipeliningLimit; return this; }
    public PgConnectOptions SetCachePreparedStatements(bool cache) { CachePreparedStatements = cache; return this; }
    public PgConnectOptions SetPreparedStatementCacheMaxSize(int size) { PreparedStatementCacheMaxSize = size; return this; }
    public PgConnectOptions SetPreparedStatementCacheSqlLimit(int limit) { PreparedStatementCacheSqlLimit = limit; return this; }
    public PgConnectOptions SetReconnectAttempts(int attempts) { ReconnectAttempts = attempts; return this; }
    public PgConnectOptions SetReconnectInterval(TimeSpan interval) { ReconnectInterval = interval; return this; }

    public PgConnectOptions AddProperty(string key, string value)
    {
        Properties[key] = value;
        return this;
    }

    public bool IsUsingDomainSocket => Host.StartsWith('/');

    public override bool Equals(object? obj)
    {
        if (obj is not PgConnectOptions other) return false;
        return Host == other.Host &&
               Port == other.Port &&
               User == other.User &&
               Password == other.Password &&
               Database == other.Database &&
               SslMode == other.SslMode &&
               UseLayer7Proxy == other.UseLayer7Proxy &&
               PipeliningLimit == other.PipeliningLimit;
    }

    public override int GetHashCode() => HashCode.Combine(Host, Port, User, Database, SslMode, PipeliningLimit);
}
