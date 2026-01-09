// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

#if PGCLIENT

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Vertx.PgClient;
using PgTuple = Vertx.PgClient.Tuple;

namespace PlatformBenchmarks;

public sealed class RawDb
{
    private readonly MemoryCache _cache
        = new(new MemoryCacheOptions { ExpirationScanFrequency = TimeSpan.FromMinutes(60) });

    private readonly PgPool _pool;

    public RawDb(AppSettings appSettings)
    {
        var connectOptions = PgConnectOptions.FromUri(appSettings.ConnectionString)
            .SetCachePreparedStatements(true);
        
        var poolOptions = new PgPoolOptions
        {
            MaxSize = 64,
            Pipelined = true,
        };

        _pool = PgPool.Create(connectOptions, poolOptions);
    }

    public async Task<World> LoadSingleQueryRow()
    {
        var id = Random.Shared.Next(1, 10001);
        var result = await _pool.PreparedQueryAsync(
            "SELECT id, randomnumber FROM world WHERE id = $1",
            PgTuple.Create(id));

        return ReadSingleRow(result);
    }

    public Task<CachedWorld[]> LoadCachedQueries(int count)
    {
        var result = new CachedWorld[count];
        var cacheKeys = _cacheKeys;
        var cache = _cache;

        for (var i = 0; i < result.Length; i++)
        {
            var id = Random.Shared.Next(1, 10001);
            var key = cacheKeys[id];
            if (cache.TryGetValue(key, out var cached))
            {
                result[i] = (CachedWorld)cached;
            }
            else
            {
                return LoadUncachedQueries(id, i, count, this, result);
            }
        }

        return Task.FromResult(result);

        static async Task<CachedWorld[]> LoadUncachedQueries(int id, int i, int count, RawDb rawdb, CachedWorld[] result)
        {
            var cacheKeys = _cacheKeys;
            var key = cacheKeys[id];

            for (; i < result.Length; i++)
            {
                async Task<CachedWorld> create(ICacheEntry _)
                {
                    var queryResult = await rawdb._pool.PreparedQueryAsync(
                        "SELECT id, randomnumber FROM world WHERE id = $1",
                        PgTuple.Create(id));
                    var row = queryResult[0];
                    return new CachedWorld { Id = row.GetValue(0).GetInteger(), RandomNumber = row.GetValue(1).GetInteger() };
                }

                result[i] = await rawdb._cache.GetOrCreateAsync(key, create);

                id = Random.Shared.Next(1, 10001);
                key = cacheKeys[id];
            }

            return result;
        }
    }

    public async Task PopulateCache()
    {
        var cacheKeys = _cacheKeys;
        var cache = _cache;
        
        for (var i = 1; i < 10001; i++)
        {
            var result = await _pool.PreparedQueryAsync(
                "SELECT id, randomnumber FROM world WHERE id = $1",
                PgTuple.Create(i));
            var row = result[0];
            cache.Set<CachedWorld>(cacheKeys[i], new CachedWorld { Id = row.GetValue(0).GetInteger(), RandomNumber = row.GetValue(1).GetInteger() });
        }

        Console.WriteLine("Caching Populated");
    }

    public async Task<World[]> LoadMultipleQueriesRows(int count)
    {
        var results = new World[count];

        // It is not acceptable to execute multiple SELECTs within a single complex query.
        // It is not acceptable to retrieve all required rows using a SELECT ... WHERE id IN (...) clause.
        // Pipelining of network traffic between the application and database is permitted.
        
        for (var i = 0; i < results.Length; i++)
        {
            var id = Random.Shared.Next(1, 10001);
            var result = await _pool.PreparedQueryAsync(
                "SELECT id, randomnumber FROM world WHERE id = $1",
                PgTuple.Create(id));
            results[i] = ReadSingleRow(result);
        }

        return results;
    }

    public async Task<World[]> LoadMultipleUpdatesRows(int count)
    {
        var results = new World[count];

        var ids = new int[count];
        var numbers = new int[count];

        for (var i = 0; i < count; i++)
        {
            ids[i] = Random.Shared.Next(1, 10001);
        }
        Array.Sort(ids);
        
        // Ensure unique ids by incrementing duplicates
        for (var i = 1; i < count; i++)
            if (ids[i] == ids[i - 1])
                ids[i] = (ids[i] % 10000) + 1;

        // Each row must be selected randomly using one query in the same fashion as the single database query test
        // Use of IN clauses or similar means to consolidate multiple queries into one operation is not permitted.
        // Similarly, use of a batch or multiple SELECTs within a single statement are not permitted
        for (var i = 0; i < results.Length; i++)
        {
            var result = await _pool.PreparedQueryAsync(
                "SELECT id, randomnumber FROM world WHERE id = $1",
                PgTuple.Create(ids[i]));
            results[i] = ReadSingleRow(result);
        }

        for (var i = 0; i < count; i++)
        {
            var randomNumber = Random.Shared.Next(1, 10001);
            if (results[i].RandomNumber == randomNumber)
            {
                randomNumber = (randomNumber % 10000) + 1;
            }

            results[i].RandomNumber = randomNumber;
            numbers[i] = randomNumber;
        }

        // Use unnest for batch update
        await _pool.PreparedQueryAsync(
            "UPDATE world w SET randomnumber = u.new_val FROM (SELECT unnest($1::int[]) as id, unnest($2::int[]) as new_val) u WHERE w.id = u.id",
            PgTuple.Create(ids, numbers));

        return results;
    }

    public async Task<List<FortuneUtf8>> LoadFortunesRows()
    {
        // Benchmark requirements explicitly prohibit pre-initializing the list size
        var result = new List<FortuneUtf8>();

        var rows = await _pool.QueryAsync("SELECT id, message FROM fortune");

        foreach (var row in rows)
        {
            result.Add(new FortuneUtf8
            (
                id: row.GetValue(0).GetInteger(),
                message: row.GetValue(1).GetBytes()!
            ));
        }

        result.Add(new FortuneUtf8(id: 0, AdditionalFortune));
        result.Sort();

        return result;
    }

    private readonly byte[] AdditionalFortune = "Additional fortune added at request time."u8.ToArray();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static World ReadSingleRow(RowSet result)
    {
        var row = result[0];
        return new World
        {
            Id = row.GetValue(0).GetInteger(),
            RandomNumber = row.GetValue(1).GetInteger()
        };
    }

    private static readonly object[] _cacheKeys = Enumerable.Range(0, 10001).Select((i) => new CacheKey(i)).ToArray();

    public sealed class CacheKey : IEquatable<CacheKey>
    {
        private readonly int _value;

        public CacheKey(int value)
            => _value = value;

        public bool Equals(CacheKey key)
            => key._value == _value;

        public override bool Equals(object obj)
            => ReferenceEquals(obj, this);

        public override int GetHashCode()
            => _value;

        public override string ToString()
            => _value.ToString();
    }
}

#endif