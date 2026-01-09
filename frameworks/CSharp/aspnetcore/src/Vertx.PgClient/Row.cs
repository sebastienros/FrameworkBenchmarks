// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using Vertx.PgClient.Codec;

namespace Vertx.PgClient;

/// <summary>
/// A row in a query result.
/// </summary>
public interface IRow
{
    /// <summary>
    /// Gets the column name at the specified position.
    /// </summary>
    string GetColumnName(int position);

    /// <summary>
    /// Gets the column index for the specified column name, or -1 if not found.
    /// </summary>
    int GetColumnIndex(string name);

    /// <summary>
    /// Gets the value at the specified position.
    /// </summary>
    PgValue GetValue(int position);

    /// <summary>
    /// Gets the value by column name.
    /// </summary>
    /// <exception cref="KeyNotFoundException">Thrown when the column name is not found.</exception>
    PgValue GetValue(string columnName);

    /// <summary>
    /// Tries to get the value by column name.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <param name="value">The value if found.</param>
    /// <returns>True if the column exists, false otherwise.</returns>
    bool TryGetValue(string columnName, out PgValue value);

    /// <summary>
    /// Checks if a column with the specified name exists.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>True if the column exists, false otherwise.</returns>
    bool Has(string name);
}

/// <summary>
/// PostgreSQL row implementation using PgValue for no-boxing storage.
/// </summary>
public sealed class PgRow : IRow
{
    private readonly PgValue[] _values;
    private readonly PgColumnDesc[] _descriptor;

    internal PgRow(PgColumnDesc[] descriptor)
    {
        _descriptor = descriptor;
        _values = new PgValue[descriptor.Length];
    }

    public int Size => _values.Length;

    /// <summary>
    /// Gets the PgValue at the specified position (no boxing).
    /// </summary>
    public PgValue GetValue(int position) => _values[position];

    internal void SetValue(int position, PgValue value) => _values[position] = value;

    public string GetColumnName(int position) => _descriptor[position].Name;

    public int GetColumnIndex(string name)
    {
        for (int i = 0; i < _descriptor.Length; i++)
        {
            if (string.Equals(_descriptor[i].Name, name, StringComparison.OrdinalIgnoreCase))
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
