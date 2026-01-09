// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using Vertx.PgClient.Codec;

namespace Vertx.PgClient;

/// <summary>
/// Represents a command that can be scheduled for execution on a PostgreSQL connection.
/// Commands support pipelining - multiple commands can be sent before waiting for responses.
/// </summary>
internal abstract class PgCommand
{
    /// <summary>
    /// Encodes the command to the encoder buffer.
    /// </summary>
    public abstract void Encode(PgEncoder encoder);

    /// <summary>
    /// Handles a response message from the server.
    /// </summary>
    /// <returns>True if the command is complete and should be removed from the inflight queue.</returns>
    public abstract bool HandleResponse(Response response);

    /// <summary>
    /// Called when the command completes successfully or with an error.
    /// </summary>
    public abstract void Complete(Exception? error = null);

    /// <summary>
    /// Gets whether this command expects a ReadyForQuery message to complete.
    /// </summary>
    public virtual bool ExpectsReadyForQuery => true;
}

/// <summary>
/// A simple query command using the simple query protocol.
/// </summary>
internal sealed class SimpleQueryCommand : PgCommand
{
    private readonly string _sql;
    private readonly TaskCompletionSource<RowSet> _tcs;
    private readonly List<Row> _rows = new();
    private PgColumnDesc[]? _columnDesc;
    private int _rowsAffected;
    private PgException? _error;

    public SimpleQueryCommand(string sql)
    {
        _sql = sql;
        _tcs = new TaskCompletionSource<RowSet>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public Task<RowSet> Task => _tcs.Task;

    public override void Encode(PgEncoder encoder)
    {
        encoder.WriteQuery(_sql);
    }

    public override bool HandleResponse(Response response)
    {
        switch (response)
        {
            case RowDescriptionResponse rd:
                _columnDesc = rd.Columns;
                return false;

            case DataRowResponse dataRow:
                if (_columnDesc is not null)
                {
                    var row = DecodeRow(dataRow.Values, _columnDesc);
                    _rows.Add(row);
                }
                return false;

            case CommandCompleteResponse cmd:
                _rowsAffected = cmd.RowsAffected;
                return false;

            case EmptyQueryResponse:
                return false;

            case ErrorResponse error:
                _error = new PgException(error.Message, error.Code, error.Severity);
                return false;

            case ReadyForQueryResponse:
                return true; // Command complete

            default:
                return false;
        }
    }

    public override void Complete(Exception? error = null)
    {
        if (error is not null)
        {
            _tcs.TrySetException(error);
        }
        else if (_error is not null)
        {
            _tcs.TrySetException(_error);
        }
        else
        {
            _tcs.TrySetResult(new RowSet(_rows, _columnDesc ?? PgColumnDesc.EmptyColumns, _rowsAffected));
        }
    }

    private static Row DecodeRow(byte[][] values, PgColumnDesc[] columnDesc)
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

        return new Row(decodedValues, columnDesc);
    }
}

/// <summary>
/// A prepared query command using the extended query protocol.
/// Handles Parse, Describe, Bind, Execute sequence with optional caching.
/// </summary>
internal sealed class PreparedQueryCommand : PgCommand
{
    private readonly string _sql;
    private readonly Tuple? _parameters;
    private readonly PreparedStatementCache? _cache;
    private readonly TaskCompletionSource<RowSet> _tcs;
    private readonly List<Row> _rows = new();

    private byte[] _statementName = null!;
    private DataType[]? _paramTypes;
    private PgColumnDesc[]? _rowDesc;
    private int _rowsAffected;
    private PgException? _error;
    private Phase _phase;
    private bool _shouldCache;
    private CachedPreparedStatement? _cachedStatement;

    private enum Phase
    {
        ParseDescribe,  // Waiting for ParseComplete, ParameterDescription, RowDescription, ReadyForQuery
        BindExecute     // Waiting for BindComplete, DataRows, CommandComplete, ReadyForQuery
    }

    public PreparedQueryCommand(string sql, Tuple? parameters, PreparedStatementCache? cache)
    {
        _sql = sql;
        _parameters = parameters;
        _cache = cache;
        _tcs = new TaskCompletionSource<RowSet>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Check cache first
        if (_cache is not null && _cache.TryGet(sql, out _cachedStatement))
        {
            _phase = Phase.BindExecute;
            _rowDesc = _cachedStatement!.RowDescription;
        }
        else
        {
            _phase = Phase.ParseDescribe;
            _shouldCache = _cache?.ShouldCache(sql) ?? false;
        }
    }

    public Task<RowSet> Task => _tcs.Task;

    public override void Encode(PgEncoder encoder)
    {
        if (_cachedStatement is not null)
        {
            // Use cached statement - just bind and execute
            encoder.WriteBind(_cachedStatement.StatementName, "", _parameters, _cachedStatement.ParameterTypes);
            encoder.WriteExecute();

            // Close any evicted statements
            if (_cache is not null)
            {
                foreach (var stmtToClose in _cache.GetStatementsToClose())
                {
                    encoder.WriteClose('S', stmtToClose);
                }
            }

            encoder.WriteSync();
        }
        else
        {
            // Need to parse first - generate statement name
            Span<byte> nameBuffer = stackalloc byte[10];
            var nameLength = encoder.GenerateStatementName(nameBuffer);
            _statementName = nameBuffer[..nameLength].ToArray();

            // Parse and describe
            encoder.WriteParse(_sql, _statementName);
            encoder.WriteDescribe('S', _statementName);
            encoder.WriteSync();
        }
    }

    /// <summary>
    /// Encodes the bind/execute phase after receiving parse/describe results.
    /// Called by the command sender when transitioning phases.
    /// </summary>
    public void EncodeBindExecute(PgEncoder encoder)
    {
        // Update row description to binary format since we request binary results in Bind
        if (_rowDesc is not null)
        {
            for (int i = 0; i < _rowDesc.Length; i++)
            {
                _rowDesc[i] = _rowDesc[i].ToBinaryDataFormat();
            }
        }

        // Cache if appropriate
        if (_shouldCache && _cache is not null)
        {
            _cache.Add(_sql, new CachedPreparedStatement
            {
                StatementName = _statementName,
                ParameterTypes = _paramTypes,
                RowDescription = _rowDesc
            });
        }

        encoder.WriteBind(_statementName, "", _parameters, _paramTypes);
        encoder.WriteExecute();

        // Close statement if not caching
        if (!_shouldCache)
        {
            encoder.WriteClose('S', _statementName);
        }

        // Close any evicted statements
        if (_cache is not null)
        {
            foreach (var stmtToClose in _cache.GetStatementsToClose())
            {
                encoder.WriteClose('S', stmtToClose);
            }
        }

        encoder.WriteSync();
        _phase = Phase.BindExecute;
    }

    /// <summary>
    /// Gets whether this command needs a second send phase (bind/execute after parse/describe).
    /// </summary>
    public bool NeedsSecondPhase => _phase == Phase.ParseDescribe;

    /// <summary>
    /// Gets whether the parse/describe phase is complete and ready for bind/execute.
    /// </summary>
    public bool ParseDescribeComplete { get; private set; }

    public override bool HandleResponse(Response response)
    {
        switch (response)
        {
            case ParseCompleteResponse:
                return false;

            case ParameterDescriptionResponse paramDesc:
                _paramTypes = paramDesc.TypeOids.Select(DataType.LookupByOid).ToArray();
                return false;

            case RowDescriptionResponse rd:
                _rowDesc = rd.Columns;
                return false;

            case NoDataResponse:
                return false;

            case BindCompleteResponse:
                return false;

            case DataRowResponse dataRow:
                if (_rowDesc is not null)
                {
                    var row = DecodeRow(dataRow.Values, _rowDesc);
                    _rows.Add(row);
                }
                return false;

            case CommandCompleteResponse cmd:
                _rowsAffected = cmd.RowsAffected;
                return false;

            case CloseCompleteResponse:
                return false;

            case ErrorResponse error:
                _error = new PgException(error.Message, error.Code, error.Severity);
                return false;

            case ReadyForQueryResponse:
                if (_phase == Phase.ParseDescribe)
                {
                    // First ReadyForQuery - parse/describe complete
                    ParseDescribeComplete = true;
                    return false; // Not done yet, need bind/execute
                }
                else
                {
                    // Second ReadyForQuery - all done
                    return true;
                }

            default:
                return false;
        }
    }

    public override void Complete(Exception? error = null)
    {
        if (error is not null)
        {
            _tcs.TrySetException(error);
        }
        else if (_error is not null)
        {
            _tcs.TrySetException(_error);
        }
        else
        {
            _tcs.TrySetResult(new RowSet(_rows, _rowDesc ?? PgColumnDesc.EmptyColumns, _rowsAffected));
        }
    }

    private static Row DecodeRow(byte[][] values, PgColumnDesc[] columnDesc)
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

        return new Row(decodedValues, columnDesc);
    }
}
