// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using Vertx.PgClient.Codec;

namespace Vertx.PgClient;

/// <summary>
/// Cached metadata for a prepared statement.
/// </summary>
internal sealed class CachedPreparedStatement
{
    public required byte[] StatementName { get; init; }
    public required DataType[]? ParameterTypes { get; init; }
    public required PgColumnDesc[]? RowDescription { get; init; }
}

/// <summary>
/// LRU cache for prepared statements.
/// </summary>
internal sealed class PreparedStatementCache
{
    private readonly int _maxSize;
    private readonly int _sqlLimit;
    private readonly Dictionary<string, LinkedListNode<CacheEntry>> _cache;
    private readonly LinkedList<CacheEntry> _lruList;
    private readonly Queue<byte[]> _statementsToClose;

    private sealed class CacheEntry
    {
        public required string Sql { get; init; }
        public required CachedPreparedStatement Statement { get; init; }
    }

    public PreparedStatementCache(int maxSize, int sqlLimit)
    {
        _maxSize = maxSize;
        _sqlLimit = sqlLimit;
        _cache = new Dictionary<string, LinkedListNode<CacheEntry>>(maxSize);
        _lruList = new LinkedList<CacheEntry>();
        _statementsToClose = new Queue<byte[]>();
    }

    /// <summary>
    /// Tries to get a cached prepared statement.
    /// </summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="statement">The cached statement if found.</param>
    /// <returns>True if the statement was found in the cache.</returns>
    public bool TryGet(string sql, out CachedPreparedStatement? statement)
    {
        if (_cache.TryGetValue(sql, out var node))
        {
            // Move to front (most recently used)
            _lruList.Remove(node);
            _lruList.AddFirst(node);
            statement = node.Value.Statement;
            return true;
        }

        statement = null;
        return false;
    }

    /// <summary>
    /// Adds a prepared statement to the cache.
    /// </summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="statement">The statement metadata to cache.</param>
    public void Add(string sql, CachedPreparedStatement statement)
    {
        // Don't cache if SQL is too long
        if (sql.Length > _sqlLimit)
        {
            return;
        }

        // Don't add duplicates
        if (_cache.ContainsKey(sql))
        {
            return;
        }

        // Evict oldest if at capacity
        while (_cache.Count >= _maxSize && _lruList.Last is not null)
        {
            var oldest = _lruList.Last;
            _cache.Remove(oldest.Value.Sql);
            _lruList.RemoveLast();
            // Queue the statement for closing
            _statementsToClose.Enqueue(oldest.Value.Statement.StatementName);
        }

        // Add new entry at front
        var entry = new CacheEntry { Sql = sql, Statement = statement };
        var node = new LinkedListNode<CacheEntry>(entry);
        _lruList.AddFirst(node);
        _cache[sql] = node;
    }

    /// <summary>
    /// Gets whether the SQL should be cached based on length limit.
    /// </summary>
    public bool ShouldCache(string sql) => sql.Length <= _sqlLimit;

    /// <summary>
    /// Gets statements that need to be closed on the server (due to eviction).
    /// </summary>
    /// <returns>Statements to close, or empty if none.</returns>
    public IReadOnlyList<byte[]> GetStatementsToClose()
    {
        if (_statementsToClose.Count == 0)
        {
            return Array.Empty<byte[]>();
        }

        var result = new List<byte[]>(_statementsToClose.Count);
        while (_statementsToClose.TryDequeue(out var name))
        {
            result.Add(name);
        }
        return result;
    }

    /// <summary>
    /// Clears the cache.
    /// </summary>
    public void Clear()
    {
        foreach (var node in _lruList)
        {
            _statementsToClose.Enqueue(node.Statement.StatementName);
        }
        _cache.Clear();
        _lruList.Clear();
    }

    /// <summary>
    /// Gets the current number of cached statements.
    /// </summary>
    public int Count => _cache.Count;
}
