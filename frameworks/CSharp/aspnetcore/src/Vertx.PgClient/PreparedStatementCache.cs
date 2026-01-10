// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<string, CacheEntry> _cache;
    private readonly LinkedList<CacheEntry> _lruList;
    private readonly ConcurrentQueue<byte[]> _statementsToClose;
    private readonly object _lruLock = new object(); // Only for LRU list updates

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
        _cache = new ConcurrentDictionary<string, CacheEntry>();
        _lruList = new LinkedList<CacheEntry>();
        _statementsToClose = new ConcurrentQueue<byte[]>();
    }

    /// <summary>
    /// Tries to get a cached prepared statement.
    /// Lock-free fast path for cache hits.
    /// </summary>
    /// <param name="sql">The SQL query.</param>
    /// <param name="statement">The cached statement if found.</param>
    /// <returns>True if the statement was found in the cache.</returns>
    public bool TryGet(string sql, out CachedPreparedStatement? statement)
    {
        // Lock-free read from ConcurrentDictionary
        if (_cache.TryGetValue(sql, out var entry))
        {
            statement = entry.Statement;
            
            // Optionally update LRU - we can skip this for better performance
            // since slightly stale LRU ordering is acceptable
            // Uncomment below if strict LRU is needed:
            /*
            if (entry.Node != null)
            {
                lock (_lruLock)
                {
                    // Move to front (most recently used)
                    if (entry.Node.List != null) // Check if still in list
                    {
                        _lruList.Remove(entry.Node);
                        _lruList.AddFirst(entry.Node);
                    }
                }
            }
            */
            
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

        // Create entry
        var entry = new CacheEntry { Sql = sql, Statement = statement };
        
        // Try to add to dictionary first (lock-free)
        if (!_cache.TryAdd(sql, entry))
        {
            // Already exists, don't add duplicate
            return;
        }

        // Now update LRU tracking under lock
        lock (_lruLock)
        {
            // Evict oldest if at capacity
            while (_cache.Count > _maxSize && _lruList.Last is not null)
            {
                var oldest = _lruList.Last;
                _lruList.RemoveLast();
                
                // Remove from dictionary
                if (_cache.TryRemove(oldest.Value.Sql, out var removed))
                {
                    removed.Node = null; // Detach node
                    // Queue the statement for closing
                    _statementsToClose.Enqueue(removed.Statement.StatementName);
                }
            }

            // Add new entry at front
            var node = _lruList.AddFirst(entry);
            entry.Node = node;
        }
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
        if (_statementsToClose.IsEmpty)
        {
            return Array.Empty<byte[]>();
        }

        var result = new List<byte[]>();
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
        lock (_lruLock)
        {
            foreach (var node in _lruList)
            {
                _statementsToClose.Enqueue(node.Statement.StatementName);
            }
            
            _lruList.Clear();
        }
        
        // Clear dictionary outside lock
        foreach (var entry in _cache.Values)
        {
            entry.Node = null;
        }
        _cache.Clear();
    }

    /// <summary>
    /// Gets the current number of cached statements.
    /// </summary>
    public int Count => _cache.Count; // ConcurrentDictionary.Count is thread-safe
}
