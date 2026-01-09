// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using Vertx.PgClient.Codec;

namespace Vertx.PgClient;

/// <summary>
/// A forward-only, streaming reader for query results.
/// Similar to ADO.NET's DbDataReader, allows processing rows one at a time
/// without buffering the entire result set in memory.
/// </summary>
public sealed class PgDataReader : IAsyncDisposable
{
    private readonly Func<PgColumnDesc[]?, CancellationToken, ValueTask<Response>> _receiveAsync;
    private readonly Action<char> _updateTransactionStatus;
    private readonly Action<NoticeResponse>? _noticeReceived;
    private readonly Action<NotificationResponse>? _notificationReceived;
    private readonly Func<CancellationToken, ValueTask> _consumeUntilReady;
    
    private PgColumnDesc[]? _columnDesc;
    private string[]? _columnNames;
    private PgValue[]? _currentRow;
    private int _rowsAffected;
    private bool _hasRows;
    private bool _isCompleted;
    private bool _isDisposed;

    /// <summary>
    /// Gets the column names for the current result set.
    /// </summary>
    public IReadOnlyList<string> ColumnNames => _columnNames ?? Array.Empty<string>();

    /// <summary>
    /// Gets the number of columns in the current result set.
    /// </summary>
    public int FieldCount => _columnDesc?.Length ?? 0;

    /// <summary>
    /// Gets the number of rows affected by the query.
    /// Only valid after all rows have been read.
    /// </summary>
    public int RowsAffected => _rowsAffected;

    /// <summary>
    /// Gets whether the result set has any rows.
    /// </summary>
    public bool HasRows => _hasRows;

    /// <summary>
    /// Gets whether reading is complete.
    /// </summary>
    public bool IsCompleted => _isCompleted;

    /// <summary>
    /// Gets the column descriptors for the current result set.
    /// </summary>
    public IReadOnlyList<PgColumnDesc>? Columns => _columnDesc;

    internal PgDataReader(
        Func<PgColumnDesc[]?, CancellationToken, ValueTask<Response>> receiveAsync,
        Action<char> updateTransactionStatus,
        Action<NoticeResponse>? noticeReceived,
        Action<NotificationResponse>? notificationReceived,
        Func<CancellationToken, ValueTask> consumeUntilReady)
    {
        _receiveAsync = receiveAsync;
        _updateTransactionStatus = updateTransactionStatus;
        _noticeReceived = noticeReceived;
        _notificationReceived = notificationReceived;
        _consumeUntilReady = consumeUntilReady;
    }

    /// <summary>
    /// Advances the reader to the next row.
    /// Returns true if there is another row, false if the result set is complete.
    /// </summary>
    public async ValueTask<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(PgDataReader));

        if (_isCompleted)
            return false;

        while (true)
        {
            var response = await _receiveAsync(_columnDesc, cancellationToken);

            switch (response)
            {
                case RowDescriptionResponse rd:
                    _columnDesc = rd.Columns;
                    _columnNames = new string[rd.Columns.Length];
                    for (int i = 0; i < rd.Columns.Length; i++)
                    {
                        _columnNames[i] = rd.Columns[i].Name;
                    }
                    break;

                case DecodedDataRowResponse decodedRow:
                    _currentRow = decodedRow.Values;
                    _hasRows = true;
                    return true;

                case DataRowResponse dataRow:
                    if (_columnDesc is not null)
                    {
                        _currentRow = DecodeRow(dataRow.Values, _columnDesc);
                        _hasRows = true;
                        return true;
                    }
                    break;

                case CommandCompleteResponse cmd:
                    _rowsAffected = cmd.RowsAffected;
                    break;

                case EmptyQueryResponse:
                    break;

                case ReadyForQueryResponse ready:
                    _updateTransactionStatus(ready.Status);
                    _isCompleted = true;
                    _currentRow = null;
                    return false;

                case ErrorResponse error:
                    await _consumeUntilReady(cancellationToken);
                    throw new PgException(error.Message, error.Code, error.Severity);

                case NoticeResponse notice:
                    _noticeReceived?.Invoke(notice);
                    break;

                case NotificationResponse notif:
                    _notificationReceived?.Invoke(notif);
                    break;

                case BindCompleteResponse:
                case CloseCompleteResponse:
                case PortalSuspendedResponse:
                    break;
            }
        }
    }

    /// <summary>
    /// Gets the value at the specified column position.
    /// </summary>
    public PgValue GetValue(int ordinal)
    {
        if (_currentRow is null)
            throw new InvalidOperationException("No row is currently available. Call ReadAsync first.");
        if (ordinal < 0 || ordinal >= _currentRow.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        return _currentRow[ordinal];
    }

    /// <summary>
    /// Gets the value by column name.
    /// </summary>
    public PgValue GetValue(string columnName)
    {
        int index = GetOrdinal(columnName);
        return GetValue(index);
    }

    /// <summary>
    /// Gets the ordinal (column index) for the specified column name.
    /// </summary>
    public int GetOrdinal(string columnName)
    {
        if (_columnNames is null)
            throw new InvalidOperationException("Column information is not available.");

        for (int i = 0; i < _columnNames.Length; i++)
        {
            if (string.Equals(_columnNames[i], columnName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
        throw new ArgumentException($"Column '{columnName}' not found.", nameof(columnName));
    }

    /// <summary>
    /// Gets the column name at the specified ordinal.
    /// </summary>
    public string GetName(int ordinal)
    {
        if (_columnNames is null)
            throw new InvalidOperationException("Column information is not available.");
        if (ordinal < 0 || ordinal >= _columnNames.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        return _columnNames[ordinal];
    }

    /// <summary>
    /// Gets whether the value at the specified ordinal is null.
    /// </summary>
    public bool IsDBNull(int ordinal) => GetValue(ordinal).IsNull;

    /// <summary>
    /// Gets the value at the specified ordinal as a boolean.
    /// </summary>
    public bool GetBoolean(int ordinal) => GetValue(ordinal).GetBoolean();

    /// <summary>
    /// Gets the value at the specified ordinal as a short.
    /// </summary>
    public short GetInt16(int ordinal) => GetValue(ordinal).GetInt16();

    /// <summary>
    /// Gets the value at the specified ordinal as an int.
    /// </summary>
    public int GetInt32(int ordinal) => GetValue(ordinal).GetInt32();

    /// <summary>
    /// Gets the value at the specified ordinal as a long.
    /// </summary>
    public long GetInt64(int ordinal) => GetValue(ordinal).GetInt64();

    /// <summary>
    /// Gets the value at the specified ordinal as a float.
    /// </summary>
    public float GetFloat(int ordinal) => GetValue(ordinal).GetFloat();

    /// <summary>
    /// Gets the value at the specified ordinal as a double.
    /// </summary>
    public double GetDouble(int ordinal) => GetValue(ordinal).GetDouble();

    /// <summary>
    /// Gets the value at the specified ordinal as a decimal.
    /// </summary>
    public decimal GetDecimal(int ordinal) => GetValue(ordinal).GetDecimal();

    /// <summary>
    /// Gets the value at the specified ordinal as a string.
    /// </summary>
    public string? GetString(int ordinal) => GetValue(ordinal).GetString();

    /// <summary>
    /// Gets the value at the specified ordinal as a DateTime.
    /// </summary>
    public DateTime GetDateTime(int ordinal) => GetValue(ordinal).GetDateTime();

    /// <summary>
    /// Gets the value at the specified ordinal as a DateTimeOffset.
    /// </summary>
    public DateTimeOffset GetDateTimeOffset(int ordinal) => GetValue(ordinal).GetDateTimeOffset();

    /// <summary>
    /// Gets the value at the specified ordinal as a Guid.
    /// </summary>
    public Guid GetGuid(int ordinal) => GetValue(ordinal).GetGuid();

    /// <summary>
    /// Gets the value at the specified ordinal as a byte array.
    /// </summary>
    public byte[]? GetBytes(int ordinal) => GetValue(ordinal).GetBytes();

    /// <summary>
    /// Gets the value at the specified ordinal as the specified type.
    /// </summary>
    public T? GetFieldValue<T>(int ordinal) => GetValue(ordinal).Get<T>();

    private static PgValue[] DecodeRow(byte[][] values, PgColumnDesc[] columnDesc)
    {
        var decodedValues = new PgValue[values.Length];
        
        for (int i = 0; i < values.Length; i++)
        {
            var column = columnDesc[i];
            if (values[i] is null)
            {
                decodedValues[i] = PgValue.CreateNull(column.DataType);
            }
            else
            {
                decodedValues[i] = column.DataFormat == DataFormat.Binary
                    ? PgValue.DecodeBinary(column.DataType, values[i])
                    : PgValue.DecodeText(column.DataType, values[i]);
            }
        }

        return decodedValues;
    }

    /// <summary>
    /// Disposes the reader and consumes any remaining data.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed)
            return;

        // Consume any remaining rows if not completed
        if (!_isCompleted)
        {
            try
            {
                await _consumeUntilReady(default);
                _isCompleted = true;
            }
            catch
            {
                // Ignore errors during cleanup
            }
        }

        _isDisposed = true;
    }
}
