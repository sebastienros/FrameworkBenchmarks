// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Text;

namespace Vertx.PgClient;

/// <summary>
/// PostgreSQL error including all fields of the ErrorResponse message
/// of the PostgreSQL frontend/backend protocol.
/// </summary>
public class PgException : Exception
{
    public string? ErrorMessage { get; }
    public string? Severity { get; }
    public string? Code { get; }
    public string? Detail { get; }
    public string? Hint { get; }
    public string? Position { get; }
    public string? InternalPosition { get; }
    public string? InternalQuery { get; }
    public string? Where { get; }
    public string? File { get; }
    public string? Line { get; }
    public string? Routine { get; }
    public string? Schema { get; }
    public string? Table { get; }
    public string? Column { get; }
    public string? DataType { get; }
    public string? Constraint { get; }

    public PgException(string? errorMessage, string? code, string? severity)
        : base(FormatMessage(errorMessage, severity, code))
    {
        ErrorMessage = errorMessage;
        Severity = severity;
        Code = code;
    }

    public PgException(string? errorMessage, string? code, string? severity, string? detail, string? hint)
        : base(FormatMessage(errorMessage, severity, code))
    {
        ErrorMessage = errorMessage;
        Severity = severity;
        Code = code;
        Detail = detail;
        Hint = hint;
    }

    public PgException(string? errorMessage, string? severity, string? code, string? detail)
        : base(FormatMessage(errorMessage, severity, code))
    {
        ErrorMessage = errorMessage;
        Severity = severity;
        Code = code;
        Detail = detail;
    }

    public PgException(
        string? errorMessage,
        string? severity,
        string? code,
        string? detail,
        string? hint,
        string? position,
        string? internalPosition,
        string? internalQuery,
        string? where,
        string? file,
        string? line,
        string? routine,
        string? schema,
        string? table,
        string? column,
        string? dataType,
        string? constraint)
        : base(FormatMessage(errorMessage, severity, code))
    {
        ErrorMessage = errorMessage;
        Severity = severity;
        Code = code;
        Detail = detail;
        Hint = hint;
        Position = position;
        InternalPosition = internalPosition;
        InternalQuery = internalQuery;
        Where = where;
        File = file;
        Line = line;
        Routine = routine;
        Schema = schema;
        Table = table;
        Column = column;
        DataType = dataType;
        Constraint = constraint;
    }

    private static string FormatMessage(string? errorMessage, string? severity, string? code)
    {
        var sb = new StringBuilder();
        if (severity is not null)
        {
            sb.Append(severity).Append(':');
        }
        if (errorMessage is not null)
        {
            if (sb.Length > 0) sb.Append(' ');
            sb.Append(errorMessage);
        }
        if (code is not null)
        {
            if (sb.Length > 0) sb.Append(' ');
            sb.Append('(').Append(code).Append(')');
        }
        return sb.ToString();
    }
}
