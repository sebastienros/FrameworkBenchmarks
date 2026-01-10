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
/// Not thread-safe - each MultiplexedConnection has its own cache accessed by a single task.
/// </summary>
internal sealed class PreparedStatementCache
{
    private readonly int _maxSize;
    private readonly int _sqlLimit;
    private readonly Dictionary<string, CacheEntry> _cache;
    private readonly LinkedList<CacheEntry> _lruList;
    private readonly List<byte[]> _statementsToClose;

    private sealed class CacheEntry
    {
        public required string Sql { get; init; }
        public required CachedPreparedStatement Statement { get; init; }
        public LinkedListNode<CacheEntry>? Node { get; set; }
    }

    public PreparedStatementCache(int maxSize, int sqlLimit)
    {
        _maxSize = maxSize;
        _sqlLimit = sqlLimit;
        _cache = new Dictionary<string, CacheEntry>(maxSize);
        _lruList = new LinkedList<CacheEntry>();
        _statementsToClose = new List<byte[]>();
    }

    /// <summary>
    /// Tries to get a cached prepared statement.
    /// </summary>
    public bool TryGet(string sql, out CachedPreparedStatement? statement)
    {
        if (_cache.TryGetValue(sql, out var entry))
        {
            statement = entry.Statement;
            
            // Move to front (most recently used)
            if (entry.Node is not null)
            {
                _lruList.Remove(entry.Node);
                _lruList.AddFirst(entry.Node);
            }
            
            return true;
        }

        statement = null;
        return false;
    }

    /// <summary>
    /// Adds a prepared statement to the cache.
    /// </summary>
    public void Add(string sql, CachedPreparedStatement statement)
    {
        // Don't cache if SQL is too long
        if (sql.Length > _sqlLimit)
        {
            return;
        }

        // Check if already exists
        if (_cache.ContainsKey(sql))
        {
            return;
        }

        // Evict oldest if at capacity
        while (_cache.Count >= _maxSize && _lruList.Last is not null)
        {
            var oldest = _lruList.Last;
            _lruList.RemoveLast();
            
            if (_cache.Remove(oldest.Value.Sql, out var removed))
            {
                removed.Node = null;
                _statementsToClose.Add(removed.Statement.StatementName);
            }
        }

        // Create and add new entry
        var entry = new CacheEntry { Sql = sql, Statement = statement };
        var node = _lruList.AddFirst(entry);
        entry.Node = node;
        _cache[sql] = entry;
    }

    /// <summary>
    /// Gets whether the SQL should be cached based on length limit.
    /// </summary>
    public bool ShouldCache(string sql) => sql.Length <= _sqlLimit;

    /// <summary>
    /// Gets statements that need to be closed on the server (due to eviction).
    /// </summary>
    public IReadOnlyList<byte[]> GetStatementsToClose()
    {
        if (_statementsToClose.Count == 0)
        {
            return Array.Empty<byte[]>();
        }

        var result = _statementsToClose.ToArray();
        _statementsToClose.Clear();
        return result;
    }

    /// <summary>
    /// Clears the cache.
    /// </summary>
    public void Clear()
    {
        foreach (var entry in _cache.Values)
        {
            _statementsToClose.Add(entry.Statement.StatementName);
            entry.Node = null;
        }
        
        _cache.Clear();
        _lruList.Clear();
    }

    /// <summary>
    /// Gets the current number of cached statements.
    /// </summary>
    public int Count => _cache.Count;
}
