// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Collections;
using Vertx.PgClient.Codec;

namespace Vertx.PgClient;

/// <summary>
/// Result of a SQL query execution.
/// </summary>
public interface IRowSet : IEnumerable<IRow>
{
    /// <summary>
    /// Gets the row descriptor for this result set.
    /// </summary>
    PgColumnDesc[] RowDescriptor { get; }

    /// <summary>
    /// Gets the number of rows affected by the query.
    /// </summary>
    int RowCount { get; }

    /// <summary>
    /// Gets the number of columns in the result.
    /// </summary>
    int ColumnCount { get; }

    /// <summary>
    /// Gets the column names.
    /// </summary>
    IReadOnlyList<string> ColumnNames { get; }

    /// <summary>
    /// Gets the next result set if this is a multi-result query.
    /// </summary>
    IRowSet? Next { get; }
}

/// <summary>
/// A set of rows from a PostgreSQL query.
/// </summary>
public sealed class RowSet : IRowSet
{
    private readonly IReadOnlyList<IRow> _rows;

    public PgColumnDesc[] RowDescriptor { get; }
    public int RowCount { get; private set; }
    public int ColumnCount => RowDescriptor.Length;
    public IRowSet? Next { get; internal set; }

    private string[]? _columnNames;
    public IReadOnlyList<string> ColumnNames => _columnNames ??= RowDescriptor.Select(c => c.Name).ToArray();

    public RowSet(PgColumnDesc[] descriptor)
    {
        _rows = new List<IRow>();
        RowDescriptor = descriptor;
    }

    public RowSet(IReadOnlyList<Row> rows, PgColumnDesc[] columns, int rowCount)
    {
        _rows = rows;
        RowDescriptor = columns;
        RowCount = rowCount;
    }

    public IEnumerator<IRow> GetEnumerator() => _rows.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Gets the number of rows in the result set.
    /// </summary>
    public int Count => _rows.Count;

    /// <summary>
    /// Gets a row by index.
    /// </summary>
    public IRow this[int index] => _rows[index];
}

/// <summary>
/// Simple row implementation using PgValue for no-boxing storage.
/// </summary>
public sealed class Row : IRow
{
    private readonly PgValue[] _values;
    private readonly PgColumnDesc[] _columns;

    public Row(PgValue[] values, PgColumnDesc[] columns)
    {
        _values = values;
        _columns = columns;
    }

    public int Size => _values.Length;

    /// <summary>
    /// Gets the PgValue at the specified position for direct no-boxing access.
    /// </summary>
    public PgValue GetValue(int position) => _values[position];

    public string GetColumnName(int position) => position >= 0 && position < _columns.Length 
        ? _columns[position].Name 
        : throw new ArgumentOutOfRangeException(nameof(position));

    public int GetColumnIndex(string name)
    {
        for (int i = 0; i < _columns.Length; i++)
        {
            if (string.Equals(_columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
        return -1;
    }

    public PgValue GetValue(string columnName)
    {
        int index = GetColumnIndex(columnName);
        if (index < 0)
        {
            throw new KeyNotFoundException($"Column '{columnName}' not found.");
        }
        return _values[index];
    }

    public bool TryGetValue(string columnName, out PgValue value)
    {
        int index = GetColumnIndex(columnName);
        if (index < 0)
        {
            value = default;
            return false;
        }
        value = _values[index];
        return true;
    }

    public bool Has(string name) => GetColumnIndex(name) >= 0;
}
