// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

#if PGCLIENT

using System.Runtime.CompilerServices;
using Minimal.Models;
using Vertx.PgClient;
using PgTuple = Vertx.PgClient.Tuple;

namespace Minimal.Database;

public class Db
{
    private static readonly Comparison<Fortune> FortuneSortComparison = (a, b) => string.CompareOrdinal(a.Message, b.Message);

    private readonly PgPool _pool;

    public Db(AppSettings appSettings)
    {
        ArgumentException.ThrowIfNullOrEmpty(appSettings.ConnectionString);

        var connectOptions = PgConnectOptions.FromUri(appSettings.ConnectionString);
        if (!appSettings.ConnectionString.Contains("cache_prepared_statements", StringComparison.OrdinalIgnoreCase))
        {
            connectOptions.CachePreparedStatements = true;
        }

        var poolOptions = PgPoolOptions.FromUri(appSettings.ConnectionString);
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

    public async Task<World[]> LoadMultipleQueriesRows(string? parameter)
    {
        var count = ParseQueries(parameter);
        var results = new World[count];

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

    public async Task<World[]> LoadMultipleUpdatesRows(string? parameter)
    {
        var count = ParseQueries(parameter);
        var results = new World[count];

        var ids = new int[count];
        var numbers = new int[count];

        for (var i = 0; i < count; i++)
        {
            ids[i] = Random.Shared.Next(1, 10001);
        }
        Array.Sort(ids);

        for (var i = 1; i < count; i++)
        {
            if (ids[i] == ids[i - 1])
            {
                ids[i] = (ids[i] % 10000) + 1;
            }
        }

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

        await _pool.PreparedQueryAsync(
            "UPDATE world w SET randomnumber = u.new_val FROM (SELECT unnest($1::int[]) as id, unnest($2::int[]) as new_val) u WHERE w.id = u.id",
            PgTuple.Create(ids, numbers));

        return results;
    }

    public async Task<List<Fortune>> LoadFortunesRows()
    {
        var result = new List<Fortune>();

        var rows = await _pool.QueryAsync("SELECT id, message FROM fortune");

        foreach (var row in rows)
        {
            result.Add(new Fortune(
                Id: row.GetValue(0).GetInteger(),
                Message: row.GetValue(1).GetString() ?? string.Empty));
        }

        result.Add(new Fortune(0, "Additional fortune added at request time."));
        result.Sort(FortuneSortComparison);

        return result;
    }

    private static int ParseQueries(string? parameter)
    {
        if (!int.TryParse(parameter, out int queries))
        {
            queries = 1;
        }
        else
        {
            queries = Math.Clamp(queries, 1, 500);
        }

        return queries;
    }

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
}

#endif
