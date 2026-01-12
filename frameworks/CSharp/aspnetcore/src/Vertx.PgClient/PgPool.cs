// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Vertx.PgClient;

/// <summary>
/// A connection pool that supports multiplexing multiple queries on shared connections.
/// When pipelining is enabled, multiple concurrent callers can share the same physical
/// socket connection, with queries being pipelined to reduce latency.
/// </summary>
public sealed class PgPool : IAsyncDisposable
{
    private readonly PgConnectOptions _connectOptions;
    private readonly PgPoolOptions _poolOptions;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _poolLock = new(1, 1);
    private readonly List<PooledConnection> _connections = new();
    private List<MultiplexedConnection> _multiplexedConnections = new();
    private readonly ConcurrentQueue<PendingRequest> _waitQueue = new();
    private readonly SemaphoreSlim _multiplexedAvailable = new(0);
    private readonly CancellationTokenSource _disposeCts = new();
    private bool _disposed;

    // Telemetry counters
    private long _totalQueriesExecuted;
    private long _connectionsCreated;

    /// <summary>
    /// Gets the total number of queries executed.
    /// </summary>
    public long TotalQueriesExecuted => Interlocked.Read(ref _totalQueriesExecuted);

    /// <summary>
    /// Gets the total number of connections created.
    /// </summary>
    public long ConnectionsCreated => Interlocked.Read(ref _connectionsCreated);

    /// <summary>
    /// Gets the total number of connections disposed.
    /// </summary>
    public long ConnectionsDisposed => Interlocked.Read(ref _connectionsDisposed);

    /// <summary>
    /// Gets the current number of connections in the pool (regular + multiplexed).
    /// </summary>
    public int Size
    {
        get
        {
            lock (_connections)
            {
                return _connections.Count + _multiplexedConnections.Count;
            }
        }
    }

    /// <summary>
    /// Gets telemetry information about multiplexed connections.
    /// </summary>
    public IEnumerable<(int InflightCount, int PipeliningLimit, int AvailableSlots)> GetMultiplexedConnectionStats()
    {
        foreach (var conn in _multiplexedConnections)
        {
            yield return (conn.InflightCount, conn.PipeliningLimit, conn.AvailableSlots);
        }
    }

    /// <summary>
    /// Gets the number of multiplexed connections.
    /// </summary>
    public int MultiplexedConnectionCount => _multiplexedConnections.Count;

    /// <summary>
    /// Gets the number of available connection slots.
    /// </summary>
    public int Available => _poolOptions.MaxSize - Size;

    /// <summary>
    /// Gets the pool options.
    /// </summary>
    public PgPoolOptions Options => _poolOptions;

    private PgPool(PgConnectOptions connectOptions, PgPoolOptions poolOptions, ILogger? logger)
    {
        _connectOptions = connectOptions;
        _poolOptions = poolOptions;
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// Creates a new connection pool.
    /// </summary>
    public static PgPool Create(PgConnectOptions connectOptions, PgPoolOptions? poolOptions = null, ILogger? logger = null)
    {
        return new PgPool(
            new PgConnectOptions(connectOptions),
            poolOptions is not null ? new PgPoolOptions(poolOptions) : new PgPoolOptions(),
            logger
        );
    }

    /// <summary>
    /// Creates a new connection pool from a connection string.
    /// </summary>
    public static PgPool Create(string connectionString, PgPoolOptions? poolOptions = null, ILogger? logger = null)
    {
        return Create(PgConnectOptions.FromUri(connectionString), poolOptions, logger);
    }

    /// <summary>
    /// Executes a simple query using a pooled connection.
    /// When pipelining is enabled, uses multiplexed connections for better throughput.
    /// The connection is automatically returned to the pool after the query completes.
    /// </summary>
    public async Task<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _totalQueriesExecuted);
        
        if (_poolOptions.Pipelined)
        {
            var connection = await AcquireMultiplexedAsync(cancellationToken);
            return await connection.QueryAsync(sql, cancellationToken);
        }

        var pooled = await AcquireAsync(cancellationToken);
        try
        {
            return await pooled.QueryAsync(sql, cancellationToken);
        }
        finally
        {
            Release(pooled);
        }
    }

    /// <summary>
    /// Executes a prepared query with parameters using a pooled connection.
    /// When pipelining is enabled, uses multiplexed connections for better throughput.
    /// The connection is automatically returned to the pool after the query completes.
    /// </summary>
    public async Task<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _totalQueriesExecuted);
        
        if (_poolOptions.Pipelined)
        {
            var connection = await AcquireMultiplexedAsync(cancellationToken);
            return await connection.PreparedQueryAsync(sql, parameters, cancellationToken);
        }

        var pooled = await AcquireAsync(cancellationToken);
        try
        {
            return await pooled.PreparedQueryAsync(sql, parameters, cancellationToken);
        }
        finally
        {
            Release(pooled);
        }
    }

    /// <summary>
    /// Executes multiple queries in a pipelined fashion using a pooled connection.
    /// </summary>
    public async ValueTask<RowSet[]> PipelineQueryAsync(string[] queries, CancellationToken cancellationToken = default)
    {
        var pooled = await AcquireAsync(cancellationToken);
        try
        {
            return await pooled.PipelineQueryAsync(queries, cancellationToken);
        }
        finally
        {
            Release(pooled);
        }
    }

    /// <summary>
    /// Executes multiple queries in a pipelined fashion using a pooled connection.
    /// </summary>
    public ValueTask<RowSet[]> PipelineQueryAsync(params string[] queries)
    {
        return PipelineQueryAsync(queries, CancellationToken.None);
    }

    /// <summary>
    /// Acquires a connection from the pool and begins a transaction.
    /// The connection is exclusively held until the transaction is committed, rolled back, or disposed.
    /// </summary>
    public async ValueTask<IPooledTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return await BeginTransactionAsync(new TransactionOptions(), cancellationToken);
    }

    /// <summary>
    /// Acquires a connection from the pool and begins a transaction with the specified options.
    /// The connection is exclusively held until the transaction is committed, rolled back, or disposed.
    /// </summary>
    public async ValueTask<IPooledTransaction> BeginTransactionAsync(TransactionOptions options, CancellationToken cancellationToken = default)
    {
        var pooled = await AcquireAsync(cancellationToken);
        try
        {
            var transaction = await pooled.BeginTransactionAsync(options, cancellationToken);
            return new PooledTransaction(this, pooled, transaction);
        }
        catch
        {
            Release(pooled);
            throw;
        }
    }

    /// <summary>
    /// Executes a function within a transaction. The transaction is automatically 
    /// committed if the function succeeds, or rolled back if an exception is thrown.
    /// </summary>
    public async ValueTask<T> WithTransactionAsync<T>(
        Func<IPgTransaction, ValueTask<T>> action, 
        CancellationToken cancellationToken = default)
    {
        return await WithTransactionAsync(action, new TransactionOptions(), cancellationToken);
    }

    /// <summary>
    /// Executes a function within a transaction with the specified options. The transaction 
    /// is automatically committed if the function succeeds, or rolled back if an exception is thrown.
    /// </summary>
    public async ValueTask<T> WithTransactionAsync<T>(
        Func<IPgTransaction, ValueTask<T>> action,
        TransactionOptions options,
        CancellationToken cancellationToken = default)
    {
        await using var transaction = await BeginTransactionAsync(options, cancellationToken);
        try
        {
            var result = await action(transaction);
            await transaction.CommitAsync(cancellationToken);
            return result;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    /// <summary>
    /// Executes an action within a transaction. The transaction is automatically 
    /// committed if the action succeeds, or rolled back if an exception is thrown.
    /// </summary>
    public async ValueTask WithTransactionAsync(
        Func<IPgTransaction, ValueTask> action,
        CancellationToken cancellationToken = default)
    {
        await WithTransactionAsync(action, new TransactionOptions(), cancellationToken);
    }

    /// <summary>
    /// Executes an action within a transaction with the specified options. The transaction 
    /// is automatically committed if the action succeeds, or rolled back if an exception is thrown.
    /// </summary>
    public async ValueTask WithTransactionAsync(
        Func<IPgTransaction, ValueTask> action,
        TransactionOptions options,
        CancellationToken cancellationToken = default)
    {
        await using var transaction = await BeginTransactionAsync(options, cancellationToken);
        try
        {
            await action(transaction);
            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    /// <summary>
    /// Acquires a multiplexed connection with available pipeline slots.
    /// Creates new connections as needed up to MaxSize.
    /// </summary>
    private async ValueTask<MultiplexedConnection> AcquireMultiplexedAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        var linkedToken = linkedCts.Token;

        while (true)
        {
            linkedToken.ThrowIfCancellationRequested();

            // Find the connection with the most available slots
            MultiplexedConnection? best = null;
            int bestAvailable = 0;

            var multiplexedConnections = _multiplexedConnections;
            foreach (var conn in multiplexedConnections)
            {
                if (!conn.IsConnected)
                    continue;

                int available = conn.AvailableSlots;

                // Can't do better than this, there is an idle connection
                if (available == conn.PipeliningLimit)
                {
                    return conn;
                }

                if (available > bestAvailable)
                {
                    best = conn;
                    bestAvailable = available;
                }
            }
            
            if (multiplexedConnections.Count < _poolOptions.MaxSize)
            {
                // We didn't find an idle connection, can we create a new multiplexed connection?
                await _poolLock.WaitAsync(linkedToken);
                try
                {
                    if (_multiplexedConnections.Count < _poolOptions.MaxSize)
                    {
                        var connection = await MultiplexedConnection.CreateAsync(_connectOptions, _logger, linkedToken);
                        Interlocked.Increment(ref _connectionsCreated);
                        
                        connection.AvailableWorker = () =>
                        {
                            _multiplexedAvailable.Release();
                            return Task.CompletedTask;
                        };

                        // Clone the list to avoid threading issues
                        _multiplexedConnections = _multiplexedConnections.ToList();
                        _multiplexedConnections.Add(connection);

                        if (_logger.IsEnabled(LogLevel.Debug))
                        {
                            _logger.LogDebug("Created new multiplexed connection. Total connections: {Size}/{MaxSize}",
                                _multiplexedConnections.Count, _poolOptions.MaxSize);
                        }

                        return connection;
                    }
                }
                finally
                {
                    _poolLock.Release();
                }
            }
            
            // The pool is full, use the best available connection
            if (best is not null)
            {
                return best;
            }

            // Pool is saturated (all connections at capacity)
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Pool saturated: all {ConnectionCount} connections at capacity, queuing on connection with less back pressure",
                    _multiplexedConnections.Count);
            }

            // We could return a random connection to spread the load
            // but instead we wait for a connection to release some commands
            // so the connection timeout has some meaning, otherwise we always get a connection immediately
            // return _multiplexedConnections[new Random().Next(_multiplexedConnections.Count)];

            // Pool is exhausted, wait for a multiplexed slot to become available
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_poolOptions.ConnectionTimeout));
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(linkedToken, timeoutCts.Token);

            try
            {
                // Wait for a signal that a slot might be available
                await _multiplexedAvailable.WaitAsync(combinedCts.Token);
                // Loop again to try acquiring a connection with available slots
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                throw new TimeoutException("Timed out waiting for a multiplexed connection slot from the pool");
            }
        }
    }

    /// <summary>
    /// Gets a dedicated connection from the pool.
    /// The caller is responsible for releasing the connection.
    /// </summary>
    public async ValueTask<IPooledConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        return await AcquireAsync(cancellationToken);
    }

    private async ValueTask<PooledConnection> AcquireAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        var linkedToken = linkedCts.Token;

        // Try to get an existing idle connection or create a new one
        while (true)
        {
            linkedToken.ThrowIfCancellationRequested();

            await _poolLock.WaitAsync(linkedToken);
            try
            {
                // Find an idle connection
                foreach (var conn in _connections)
                {
                    if (conn.TryAcquire())
                    {
                        _logger.LogTrace("Acquired existing connection from pool");
                        return conn;
                    }
                }

                // Can we create a new connection?
                if (_connections.Count < _poolOptions.MaxSize)
                {
                    var socket = new PgSocketConnection(_connectOptions, _logger);
                    await socket.ConnectAsync(linkedToken);
                    
                    var pooled = new PooledConnection(this, socket);
                    pooled.TryAcquire(); // Mark as in use
                    _connections.Add(pooled);

                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug("Created new connection. Pool size: {Size}/{MaxSize}",
                            _connections.Count, _poolOptions.MaxSize);
                    }

                    return pooled;
                }
            }
            finally
            {
                _poolLock.Release();
            }

            // Pool is exhausted, wait for a connection to become available
            if (_poolOptions.MaxWaitQueueSize > 0 && _waitQueue.Count >= _poolOptions.MaxWaitQueueSize)
            {
                throw new InvalidOperationException("Connection pool wait queue is full");
            }

            var request = new PendingRequest();
            _waitQueue.Enqueue(request);

            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_poolOptions.ConnectionTimeout));
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(linkedToken, timeoutCts.Token);

            try
            {
                await request.WaitAsync(combinedCts.Token);
                if (request.Connection is not null)
                {
                    return request.Connection;
                }
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                throw new TimeoutException("Timed out waiting for a connection from the pool");
            }
        }
    }

    private void Release(PooledConnection connection)
    {
        connection.Release();

        // Check if anyone is waiting for a connection
        while (_waitQueue.TryDequeue(out var request))
        {
            if (connection.TryAcquire())
            {
                request.Complete(connection);
                return;
            }
        }
    }

    /// <summary>
    /// Internal release method for pooled transactions.
    /// </summary>
    internal void ReleasePooledConnection(PooledConnection connection)
    {
        Release(connection);
    }

    internal void RemoveConnection(PooledConnection connection)
    {
        lock (_connections)
        {
            _connections.Remove(connection);
        }
    }

    /// <summary>
    /// Closes all connections and disposes the pool.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        _disposeCts.Cancel();

        // Complete all waiting requests with cancellation
        while (_waitQueue.TryDequeue(out var request))
        {
            request.Cancel();
        }

        // Close all connections
        List<PooledConnection> toClose;
        List<MultiplexedConnection> multiplexedToClose;
        lock (_connections)
        {
            toClose = new List<PooledConnection>(_connections);
            _connections.Clear();
            multiplexedToClose = new List<MultiplexedConnection>(_multiplexedConnections);
            _multiplexedConnections.Clear();
        }

        foreach (var conn in toClose)
        {
            try
            {
                await conn.CloseAndDisposeAsync();
            }
            catch
            {
                // Ignore errors during cleanup
            }
        }

        foreach (var conn in multiplexedToClose)
        {
            try
            {
                await conn.DisposeAsync();
            }
            catch
            {
                // Ignore errors during cleanup
            }
        }

        _poolLock.Dispose();
        _multiplexedAvailable.Dispose();
        _disposeCts.Dispose();
    }

    private sealed class PendingRequest
    {
        private readonly TaskCompletionSource<bool> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public PooledConnection? Connection { get; private set; }

        public Task WaitAsync(CancellationToken cancellationToken)
        {
            cancellationToken.Register(() => _tcs.TrySetCanceled(cancellationToken));
            return _tcs.Task;
        }

        public void Complete(PooledConnection connection)
        {
            Connection = connection;
            _tcs.TrySetResult(true);
        }

        public void Cancel()
        {
            _tcs.TrySetCanceled();
        }
    }
}

/// <summary>
/// Represents a pooled connection that can be used for queries.
/// </summary>
public interface IPooledConnection : IAsyncDisposable
{
    /// <summary>
    /// Gets whether the connection is currently valid.
    /// </summary>
    bool IsValid { get; }

    /// <summary>
    /// Executes a simple query.
    /// </summary>
    ValueTask<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a prepared query with parameters.
    /// </summary>
    ValueTask<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes multiple queries in a pipelined fashion.
    /// </summary>
    ValueTask<RowSet[]> PipelineQueryAsync(string[] queries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the connection to the pool.
    /// </summary>
    void Close();
}

/// <summary>
/// A pooled connection wrapper that tracks usage and supports multiplexing.
/// </summary>
internal sealed class PooledConnection : IPooledConnection
{
    private readonly PgPool _pool;
    private readonly PgSocketConnection _socket;
    private readonly SemaphoreSlim _commandLock = new(1, 1);
    private int _acquired; // 0 = idle, 1 = acquired for exclusive use
    private bool _disposed;

    public bool IsValid => _socket.IsConnected && !_disposed;

    internal PooledConnection(PgPool pool, PgSocketConnection socket)
    {
        _pool = pool;
        _socket = socket;
    }

    internal bool TryAcquire()
    {
        return Interlocked.CompareExchange(ref _acquired, 1, 0) == 0;
    }

    internal void Release()
    {
        Interlocked.Exchange(ref _acquired, 0);
    }

    public async ValueTask<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        await _commandLock.WaitAsync(cancellationToken);
        try
        {
            return await _socket.QueryAsync(sql, cancellationToken);
        }
        finally
        {
            _commandLock.Release();
        }
    }

    public async ValueTask<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default)
    {
        await _commandLock.WaitAsync(cancellationToken);
        try
        {
            return await _socket.PreparedQueryAsync(sql, parameters, cancellationToken);
        }
        finally
        {
            _commandLock.Release();
        }
    }

    public async ValueTask<RowSet[]> PipelineQueryAsync(string[] queries, CancellationToken cancellationToken = default)
    {
        await _commandLock.WaitAsync(cancellationToken);
        try
        {
            return await _socket.PipelineQueryAsync(queries, cancellationToken);
        }
        finally
        {
            _commandLock.Release();
        }
    }

    /// <summary>
    /// Schedules a command for pipelined execution, allowing concurrent callers.
    /// </summary>
    internal async Task<RowSet> ScheduleCommandAsync(SimpleQueryCommand command, CancellationToken cancellationToken)
    {
        // Use the socket's pipelining infrastructure
        return await _socket.ScheduleAndWaitAsync(command, cancellationToken);
    }

    /// <summary>
    /// Begins a transaction on this connection.
    /// </summary>
    internal async ValueTask<IPgTransaction> BeginTransactionAsync(TransactionOptions options, CancellationToken cancellationToken)
    {
        await _commandLock.WaitAsync(cancellationToken);
        try
        {
            if (_socket.TransactionStatus != 'I')
                throw new InvalidOperationException("Connection is already in a transaction");

            var sql = $"BEGIN {options.ToSql()}";
            await _socket.QueryAsync(sql, cancellationToken);

            return new PooledConnectionTransaction(this);
        }
        finally
        {
            _commandLock.Release();
        }
    }

    public void Close()
    {
        Release();
    }

    /// <summary>
    /// Returns the connection to the pool. Does not close the underlying socket.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        // Return to pool, don't close
        Release();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Actually closes and disposes the connection. Called by the pool during cleanup.
    /// </summary>
    internal async ValueTask CloseAndDisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        _pool.RemoveConnection(this);
        
        try
        {
            await _socket.CloseAsync();
        }
        catch
        {
            // Ignore errors during cleanup
        }

        await _socket.DisposeAsync();
        _commandLock.Dispose();
    }
}
