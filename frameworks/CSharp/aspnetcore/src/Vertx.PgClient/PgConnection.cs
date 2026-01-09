// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Vertx.PgClient.Codec;

namespace Vertx.PgClient;

/// <summary>
/// A connection to PostgreSQL database.
/// </summary>
public interface IPgConnection : IAsyncDisposable
{
    /// <summary>
    /// Gets the process ID of the backend server process handling this connection.
    /// </summary>
    int ProcessId { get; }

    /// <summary>
    /// Gets the secret key for the connection (used for cancel requests).
    /// </summary>
    int SecretKey { get; }

    /// <summary>
    /// Gets whether the connection is currently open.
    /// </summary>
    bool IsOpen { get; }

    /// <summary>
    /// Gets the current transaction status.
    /// 'I' = Idle (not in a transaction)
    /// 'T' = In a transaction block
    /// 'E' = In a failed transaction block
    /// </summary>
    char TransactionStatus { get; }

    /// <summary>
    /// Gets the database metadata.
    /// </summary>
    PgDatabaseMetadata DatabaseMetadata { get; }

    /// <summary>
    /// Gets the server parameters.
    /// </summary>
    IReadOnlyDictionary<string, string> ServerParameters { get; }

    /// <summary>
    /// Executes a simple query.
    /// </summary>
    ValueTask<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a prepared query with parameters.
    /// </summary>
    ValueTask<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a simple query and returns a streaming reader for the results.
    /// Unlike QueryAsync, this does not buffer all rows in memory.
    /// Similar to ADO.NET's ExecuteReaderAsync.
    /// </summary>
    ValueTask<PgDataReader> ExecuteReaderAsync(string sql, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a prepared query with parameters and returns a streaming reader for the results.
    /// Unlike PreparedQueryAsync, this does not buffer all rows in memory.
    /// Similar to ADO.NET's ExecuteReaderAsync.
    /// </summary>
    ValueTask<PgDataReader> ExecuteReaderAsync(string sql, Tuple? parameters, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes multiple queries in a pipelined fashion.
    /// All queries are sent before waiting for any responses, improving throughput.
    /// </summary>
    ValueTask<RowSet[]> PipelineQueryAsync(string[] queries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes multiple queries in a pipelined fashion.
    /// All queries are sent before waiting for any responses, improving throughput.
    /// </summary>
    ValueTask<RowSet[]> PipelineQueryAsync(params string[] queries);

    /// <summary>
    /// Closes the connection.
    /// </summary>
    ValueTask CloseAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Begins a new transaction with default options.
    /// </summary>
    ValueTask<IPgTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Begins a new transaction with the specified options.
    /// </summary>
    ValueTask<IPgTransaction> BeginTransactionAsync(TransactionOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Event raised when a notification is received.
    /// </summary>
    event Action<PgNotification>? NotificationReceived;

    /// <summary>
    /// Event raised when a notice is received.
    /// </summary>
    event Action<PgNotice>? NoticeReceived;
}

/// <summary>
/// PostgreSQL database metadata.
/// </summary>
public sealed class PgDatabaseMetadata
{
    internal PgDatabaseMetadata() { }

    public string? ServerVersion { get; internal set; }
    public string? ServerEncoding { get; internal set; }
    public string? ClientEncoding { get; internal set; }
    public string? TimeZone { get; internal set; }
    public string? DateStyle { get; internal set; }
    public bool IntegerDateTimes { get; internal set; }

    public string ProductName => "PostgreSQL";
    public int MajorVersion { get; internal set; }
    public int MinorVersion { get; internal set; }

    internal static PgDatabaseMetadata FromServerParameters(IReadOnlyDictionary<string, string> parameters)
    {
        var metadata = new PgDatabaseMetadata
        {
            ServerVersion = parameters.GetValueOrDefault("server_version"),
            ServerEncoding = parameters.GetValueOrDefault("server_encoding"),
            ClientEncoding = parameters.GetValueOrDefault("client_encoding"),
            TimeZone = parameters.GetValueOrDefault("TimeZone"),
            DateStyle = parameters.GetValueOrDefault("DateStyle"),
            IntegerDateTimes = parameters.GetValueOrDefault("integer_datetimes") == "on"
        };

        // Parse version
        if (metadata.ServerVersion is not null)
        {
            ReadOnlySpan<char> version = metadata.ServerVersion.AsSpan();
            Span<Range> ranges = stackalloc Range[3];
            int count = version.Split(ranges, '.');
            
            if (count >= 1 && int.TryParse(version[ranges[0]], out int major))
            {
                metadata.MajorVersion = major;
                
                if (count >= 2)
                {
                    // Handle versions like "15.2" or "15.2 (Debian)" - find end of minor version number
                    ReadOnlySpan<char> minorPart = version[ranges[1]];
                    int endIndex = 0;
                    while (endIndex < minorPart.Length && char.IsAsciiDigit(minorPart[endIndex]))
                    {
                        endIndex++;
                    }
                    
                    if (endIndex > 0 && int.TryParse(minorPart[..endIndex], out int minor))
                    {
                        metadata.MinorVersion = minor;
                    }
                }
            }
        }

        return metadata;
    }
}

/// <summary>
/// Default implementation of IPgConnection.
/// </summary>
public sealed class PgConnection : IPgConnection
{
    private readonly ILogger _logger;
    private PgSocketConnection? _socket;
    private PgDatabaseMetadata? _metadata;

    /// <inheritdoc/>
    public bool IsOpen => _socket?.IsConnected == true;

    /// <inheritdoc/>
    public int ProcessId => _socket?.ProcessId ?? 0;

    /// <inheritdoc/>
    public int SecretKey { get; private set; }

    /// <inheritdoc/>
    public char TransactionStatus => _socket?.TransactionStatus ?? 'I';

    /// <inheritdoc/>
    public PgDatabaseMetadata DatabaseMetadata => _metadata ??= 
        PgDatabaseMetadata.FromServerParameters(_socket?.ServerParameters ?? new Dictionary<string, string>());

    /// <inheritdoc/>
    public IReadOnlyDictionary<string, string> ServerParameters => 
        _socket?.ServerParameters ?? new Dictionary<string, string>();

    /// <inheritdoc/>
    public event Action<PgNotification>? NotificationReceived;

    /// <inheritdoc/>
    public event Action<PgNotice>? NoticeReceived;

    private PgConnection(ILogger? logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// Creates a new connection using the specified options.
    /// </summary>
    public static async ValueTask<IPgConnection> ConnectAsync(
        PgConnectOptions options,
        CancellationToken cancellationToken = default)
    {
        return await ConnectAsync(options, null, cancellationToken);
    }

    /// <summary>
    /// Creates a new connection using the specified options and logger.
    /// </summary>
    public static async ValueTask<IPgConnection> ConnectAsync(
        PgConnectOptions options,
        ILogger? logger,
        CancellationToken cancellationToken = default)
    {
        var connection = new PgConnection(logger);
        await connection.ConnectInternalAsync(options, cancellationToken);
        return connection;
    }

    /// <summary>
    /// Creates a new connection using a connection string.
    /// </summary>
    public static async ValueTask<IPgConnection> ConnectAsync(
        string connectionString,
        CancellationToken cancellationToken = default)
    {
        var options = PgConnectOptions.FromUri(connectionString);
        return await ConnectAsync(options, cancellationToken);
    }

    /// <summary>
    /// Creates a new connection using a connection string and logger.
    /// </summary>
    public static async ValueTask<IPgConnection> ConnectAsync(
        string connectionString,
        ILogger? logger,
        CancellationToken cancellationToken = default)
    {
        var options = PgConnectOptions.FromUri(connectionString);
        return await ConnectAsync(options, logger, cancellationToken);
    }

    private async ValueTask ConnectInternalAsync(PgConnectOptions options, CancellationToken cancellationToken)
    {
        _socket = new PgSocketConnection(options, _logger);
        
        // Wire up events
        _socket.NotificationReceived += notification => NotificationReceived?.Invoke(notification);
        _socket.NoticeReceived += notice => NoticeReceived?.Invoke(new PgNotice(
            notice.Severity,
            notice.Code,
            notice.Message,
            notice.Detail,
            notice.Hint,
            notice.File,
            notice.Line,
            notice.Routine
        ));

        await _socket.ConnectAsync(cancellationToken);
        SecretKey = _socket.SecretKey;
        _metadata = PgDatabaseMetadata.FromServerParameters(_socket.ServerParameters);
    }

    /// <inheritdoc/>
    public async ValueTask<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (_socket is null || !IsOpen)
            throw new InvalidOperationException("Connection is not open");

        return await _socket.QueryAsync(sql, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default)
    {
        if (_socket is null || !IsOpen)
            throw new InvalidOperationException("Connection is not open");

        return await _socket.PreparedQueryAsync(sql, parameters, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask<PgDataReader> ExecuteReaderAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (_socket is null || !IsOpen)
            throw new InvalidOperationException("Connection is not open");

        return await _socket.ExecuteReaderAsync(sql, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask<PgDataReader> ExecuteReaderAsync(string sql, Tuple? parameters, CancellationToken cancellationToken = default)
    {
        if (_socket is null || !IsOpen)
            throw new InvalidOperationException("Connection is not open");

        return await _socket.ExecuteReaderAsync(sql, parameters, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask<RowSet[]> PipelineQueryAsync(string[] queries, CancellationToken cancellationToken = default)
    {
        if (_socket is null || !IsOpen)
            throw new InvalidOperationException("Connection is not open");

        return await _socket.PipelineQueryAsync(queries, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<RowSet[]> PipelineQueryAsync(params string[] queries)
    {
        return PipelineQueryAsync(queries, CancellationToken.None);
    }

    /// <inheritdoc/>
    public async ValueTask CloseAsync(CancellationToken cancellationToken = default)
    {
        if (_socket is not null)
        {
            await _socket.CloseAsync(cancellationToken);
            _socket = null;
        }
    }

    /// <inheritdoc/>
    public ValueTask<IPgTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return BeginTransactionAsync(new TransactionOptions(), cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask<IPgTransaction> BeginTransactionAsync(TransactionOptions options, CancellationToken cancellationToken = default)
    {
        if (_socket is null || !IsOpen)
            throw new InvalidOperationException("Connection is not open");

        if (TransactionStatus != 'I')
            throw new InvalidOperationException("Connection is already in a transaction");

        var sql = $"BEGIN {options.ToSql()}";
        await _socket.QueryAsync(sql, cancellationToken);

        return new PgTransaction(this);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_socket is not null)
        {
            await _socket.DisposeAsync();
            _socket = null;
        }
    }
}
