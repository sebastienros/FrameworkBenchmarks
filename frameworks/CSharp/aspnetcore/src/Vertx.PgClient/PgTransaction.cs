// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient;

/// <summary>
/// Represents a database transaction.
/// </summary>
public interface IPgTransaction : IAsyncDisposable
{
    /// <summary>
    /// Gets the connection associated with this transaction.
    /// </summary>
    IPgConnection Connection { get; }

    /// <summary>
    /// Gets whether the transaction is active (not committed or rolled back).
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Commits the transaction.
    /// </summary>
    ValueTask CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back the transaction.
    /// </summary>
    ValueTask RollbackAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a savepoint with the specified name.
    /// </summary>
    ValueTask SavepointAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back to the specified savepoint.
    /// </summary>
    ValueTask RollbackToSavepointAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the specified savepoint.
    /// </summary>
    ValueTask ReleaseSavepointAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a simple query within this transaction.
    /// </summary>
    ValueTask<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a prepared query with parameters within this transaction.
    /// </summary>
    ValueTask<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents a transaction obtained from a connection pool.
/// When disposed, the underlying connection is returned to the pool.
/// </summary>
public interface IPooledTransaction : IPgTransaction
{
}

/// <summary>
/// Transaction isolation levels.
/// </summary>
public enum IsolationLevel
{
    /// <summary>
    /// Read uncommitted isolation level (PostgreSQL treats as ReadCommitted).
    /// </summary>
    ReadUncommitted,

    /// <summary>
    /// Read committed isolation level (PostgreSQL default).
    /// </summary>
    ReadCommitted,

    /// <summary>
    /// Repeatable read isolation level.
    /// </summary>
    RepeatableRead,

    /// <summary>
    /// Serializable isolation level.
    /// </summary>
    Serializable
}

/// <summary>
/// Transaction access mode.
/// </summary>
public enum TransactionAccessMode
{
    /// <summary>
    /// Read-write transaction (default).
    /// </summary>
    ReadWrite,

    /// <summary>
    /// Read-only transaction.
    /// </summary>
    ReadOnly
}

/// <summary>
/// Options for configuring a transaction.
/// </summary>
public sealed class TransactionOptions
{
    /// <summary>
    /// Gets or sets the isolation level for the transaction.
    /// </summary>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

    /// <summary>
    /// Gets or sets the access mode for the transaction.
    /// </summary>
    public TransactionAccessMode AccessMode { get; set; } = TransactionAccessMode.ReadWrite;

    /// <summary>
    /// Gets or sets whether the transaction is deferrable.
    /// Only applicable for serializable read-only transactions.
    /// </summary>
    public bool Deferrable { get; set; }

    internal string ToSql()
    {
        var parts = new List<string>();

        parts.Add(IsolationLevel switch
        {
            IsolationLevel.ReadUncommitted => "ISOLATION LEVEL READ UNCOMMITTED",
            IsolationLevel.ReadCommitted => "ISOLATION LEVEL READ COMMITTED",
            IsolationLevel.RepeatableRead => "ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel.Serializable => "ISOLATION LEVEL SERIALIZABLE",
            _ => "ISOLATION LEVEL READ COMMITTED"
        });

        parts.Add(AccessMode switch
        {
            TransactionAccessMode.ReadOnly => "READ ONLY",
            TransactionAccessMode.ReadWrite => "READ WRITE",
            _ => "READ WRITE"
        });

        if (Deferrable && IsolationLevel == IsolationLevel.Serializable && AccessMode == TransactionAccessMode.ReadOnly)
        {
            parts.Add("DEFERRABLE");
        }

        return string.Join(", ", parts);
    }
}

/// <summary>
/// Implementation of IPgTransaction.
/// </summary>
internal sealed class PgTransaction : IPgTransaction
{
    private readonly IPgConnection _connection;
    private bool _isActive;
    private bool _isDisposed;

    /// <inheritdoc/>
    public IPgConnection Connection => _connection;

    /// <inheritdoc/>
    public bool IsActive => _isActive && !_isDisposed;

    internal PgTransaction(IPgConnection connection)
    {
        _connection = connection;
        _isActive = true;
    }

    /// <inheritdoc/>
    public async ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        await _connection.QueryAsync("COMMIT", cancellationToken);
        _isActive = false;
    }

    /// <inheritdoc/>
    public async ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        await _connection.QueryAsync("ROLLBACK", cancellationToken);
        _isActive = false;
    }

    /// <inheritdoc/>
    public async ValueTask SavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        ValidateSavepointName(name);
        await _connection.QueryAsync($"SAVEPOINT {QuoteIdentifier(name)}", cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask RollbackToSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        ValidateSavepointName(name);
        await _connection.QueryAsync($"ROLLBACK TO SAVEPOINT {QuoteIdentifier(name)}", cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask ReleaseSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        ValidateSavepointName(name);
        await _connection.QueryAsync($"RELEASE SAVEPOINT {QuoteIdentifier(name)}", cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        return _connection.QueryAsync(sql, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        return _connection.PreparedQueryAsync(sql, parameters, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        // Rollback if still active
        if (_isActive)
        {
            try
            {
                await _connection.QueryAsync("ROLLBACK");
            }
            catch
            {
                // Ignore errors during cleanup
            }
            _isActive = false;
        }
    }

    private void ThrowIfNotActive()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(PgTransaction));
        if (!_isActive)
            throw new InvalidOperationException("Transaction is no longer active");
    }

    private static void ValidateSavepointName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Savepoint name cannot be null or empty", nameof(name));
    }

    private static string QuoteIdentifier(string identifier)
    {
        // PostgreSQL identifier quoting: double any embedded quotes and wrap in quotes
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }
}

/// <summary>
/// A transaction created on a PooledConnection.
/// Does not implement IPgConnection.Connection since PooledConnection doesn't implement IPgConnection.
/// </summary>
internal sealed class PooledConnectionTransaction : IPgTransaction
{
    private readonly PooledConnection _connection;
    private bool _isActive;
    private bool _isDisposed;

    /// <inheritdoc/>
    /// <remarks>Returns null since PooledConnection doesn't implement IPgConnection.</remarks>
    public IPgConnection Connection => null!;

    /// <inheritdoc/>
    public bool IsActive => _isActive && !_isDisposed;

    internal PooledConnectionTransaction(PooledConnection connection)
    {
        _connection = connection;
        _isActive = true;
    }

    /// <inheritdoc/>
    public async ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        await _connection.QueryAsync("COMMIT", cancellationToken);
        _isActive = false;
    }

    /// <inheritdoc/>
    public async ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        await _connection.QueryAsync("ROLLBACK", cancellationToken);
        _isActive = false;
    }

    /// <inheritdoc/>
    public async ValueTask SavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        ValidateSavepointName(name);
        await _connection.QueryAsync($"SAVEPOINT {QuoteIdentifier(name)}", cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask RollbackToSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        ValidateSavepointName(name);
        await _connection.QueryAsync($"ROLLBACK TO SAVEPOINT {QuoteIdentifier(name)}", cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask ReleaseSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        ValidateSavepointName(name);
        await _connection.QueryAsync($"RELEASE SAVEPOINT {QuoteIdentifier(name)}", cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        return _connection.QueryAsync(sql, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default)
    {
        ThrowIfNotActive();
        return _connection.PreparedQueryAsync(sql, parameters, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        // Rollback if still active
        if (_isActive)
        {
            try
            {
                await _connection.QueryAsync("ROLLBACK");
            }
            catch
            {
                // Ignore errors during cleanup
            }
            _isActive = false;
        }
    }

    private void ThrowIfNotActive()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(PooledConnectionTransaction));
        if (!_isActive)
            throw new InvalidOperationException("Transaction is no longer active");
    }

    private static void ValidateSavepointName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Savepoint name cannot be null or empty", nameof(name));
    }

    private static string QuoteIdentifier(string identifier)
    {
        // PostgreSQL identifier quoting: double any embedded quotes and wrap in quotes
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }
}

/// <summary>
/// A transaction that returns its connection to the pool when disposed.
/// </summary>
internal sealed class PooledTransaction : IPooledTransaction
{
    private readonly PgPool _pool;
    private readonly PooledConnection _pooledConnection;
    private readonly IPgTransaction _transaction;
    private bool _isDisposed;

    /// <inheritdoc/>
    public IPgConnection Connection => _transaction.Connection;

    /// <inheritdoc/>
    public bool IsActive => _transaction.IsActive;

    internal PooledTransaction(PgPool pool, PooledConnection pooledConnection, IPgTransaction transaction)
    {
        _pool = pool;
        _pooledConnection = pooledConnection;
        _transaction = transaction;
    }

    /// <inheritdoc/>
    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        return _transaction.CommitAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        return _transaction.RollbackAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask SavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        return _transaction.SavepointAsync(name, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask RollbackToSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        return _transaction.RollbackToSavepointAsync(name, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask ReleaseSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        return _transaction.ReleaseSavepointAsync(name, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<RowSet> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        return _transaction.QueryAsync(sql, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<RowSet> PreparedQueryAsync(string sql, Tuple? parameters = null, CancellationToken cancellationToken = default)
    {
        return _transaction.PreparedQueryAsync(sql, parameters, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        // Dispose the transaction (which will rollback if still active)
        await _transaction.DisposeAsync();

        // Return the connection to the pool
        _pool.ReleasePooledConnection(_pooledConnection);
    }
}
