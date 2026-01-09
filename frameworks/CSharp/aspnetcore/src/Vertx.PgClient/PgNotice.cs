// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using Microsoft.Extensions.Logging;

namespace Vertx.PgClient;

/// <summary>
/// A notice emitted by Postgres.
/// </summary>
public sealed class PgNotice
{
    public string? Severity { get; set; }
    public string? Code { get; set; }
    public string? Message { get; set; }
    public string? Detail { get; set; }
    public string? Hint { get; set; }
    public string? Position { get; set; }
    public string? InternalPosition { get; set; }
    public string? InternalQuery { get; set; }
    public string? Where { get; set; }
    public string? File { get; set; }
    public string? Line { get; set; }
    public string? Routine { get; set; }
    public string? Schema { get; set; }
    public string? Table { get; set; }
    public string? Column { get; set; }
    public string? DataType { get; set; }
    public string? Constraint { get; set; }

    public PgNotice() { }

    public PgNotice(string? severity, string? code, string? message, string? detail = null,
        string? hint = null, string? file = null, string? line = null, string? routine = null)
    {
        Severity = severity;
        Code = code;
        Message = message;
        Detail = detail;
        Hint = hint;
        File = file;
        Line = line;
        Routine = routine;
    }

    public PgNotice SetSeverity(string? value) { Severity = value; return this; }
    public PgNotice SetCode(string? value) { Code = value; return this; }
    public PgNotice SetMessage(string? value) { Message = value; return this; }
    public PgNotice SetDetail(string? value) { Detail = value; return this; }
    public PgNotice SetHint(string? value) { Hint = value; return this; }
    public PgNotice SetPosition(string? value) { Position = value; return this; }
    public PgNotice SetInternalPosition(string? value) { InternalPosition = value; return this; }
    public PgNotice SetInternalQuery(string? value) { InternalQuery = value; return this; }
    public PgNotice SetWhere(string? value) { Where = value; return this; }
    public PgNotice SetFile(string? value) { File = value; return this; }
    public PgNotice SetLine(string? value) { Line = value; return this; }
    public PgNotice SetRoutine(string? value) { Routine = value; return this; }
    public PgNotice SetSchema(string? value) { Schema = value; return this; }
    public PgNotice SetTable(string? value) { Table = value; return this; }
    public PgNotice SetColumn(string? value) { Column = value; return this; }
    public PgNotice SetDataType(string? value) { DataType = value; return this; }
    public PgNotice SetConstraint(string? value) { Constraint = value; return this; }

    public void Log(ILogger? logger)
    {
        if (logger is null) return;

        var logLevel = Severity?.ToUpperInvariant() switch
        {
            "WARNING" => LogLevel.Warning,
            "NOTICE" => LogLevel.Information,
            "DEBUG" => LogLevel.Debug,
            "INFO" => LogLevel.Information,
            "LOG" => LogLevel.Information,
            _ => LogLevel.Information
        };

        logger.Log(logLevel, "PostgreSQL Notice: {Severity} - {Code}: {Message}", Severity, Code, Message);
    }
}
