// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Codec;

/// <summary>
/// Base class for all PostgreSQL backend responses.
/// </summary>
internal abstract record Response;

/// <summary>
/// PostgreSQL error response.
/// </summary>
internal sealed record ErrorResponse(
    string Severity,
    string Code,
    string Message,
    string? Detail = null,
    string? Hint = null,
    string? Position = null,
    string? InternalQuery = null,
    string? Where = null,
    string? Schema = null,
    string? Table = null,
    string? Column = null,
    string? DataType = null,
    string? Constraint = null,
    string? File = null,
    string? Line = null,
    string? Routine = null
) : Response
{
    public PgException ToException() => new(Message, Code, Severity, Detail, Hint);
}

/// <summary>
/// PostgreSQL notice response.
/// </summary>
internal sealed record NoticeResponse(
    string Severity,
    string Code,
    string Message,
    string? Detail = null,
    string? Hint = null,
    string? Position = null,
    string? Where = null,
    string? File = null,
    string? Line = null,
    string? Routine = null
) : Response
{
    public PgNotice ToNotice() => new(Severity, Code, Message, Detail, Hint, File, Line, Routine);
}

// Protocol response records
internal sealed record BindCompleteResponse : Response;
internal sealed record CloseCompleteResponse : Response;
internal sealed record CommandCompleteResponse(int RowsAffected) : Response;
internal sealed record CopyDataResponse(byte[] Data) : Response;
internal sealed record CopyDoneResponse : Response;
internal sealed record CopyResponse(bool IsBinary, short[] Formats, bool IsCopyIn) : Response;
internal sealed record DataRowResponse(byte[][] Values) : Response;
/// <summary>
/// A decoded data row response that skips intermediate byte[] allocations.
/// Contains pre-decoded PgValue array.
/// </summary>
internal sealed record DecodedDataRowResponse(PgValue[] Values) : Response;
internal sealed record EmptyQueryResponse : Response;
internal sealed record NoDataResponse : Response;
internal sealed record ParseCompleteResponse : Response;
internal sealed record PortalSuspendedResponse : Response;
internal sealed record UnknownResponse(byte Type, byte[] Payload) : Response;

// Authentication responses
internal sealed record AuthenticationOkResponse : Response;
internal sealed record AuthenticationCleartextPasswordResponse : Response;
internal sealed record AuthenticationMd5PasswordResponse(byte[] Salt) : Response;
internal sealed record AuthenticationSASLResponse(string[] Mechanisms) : Response;
internal sealed record AuthenticationSASLContinueResponse(byte[] Data) : Response;
internal sealed record AuthenticationSASLFinalResponse(byte[] Data) : Response;
internal sealed record AuthenticationUnknownResponse(int Type) : Response;

// Backend information responses
internal sealed record BackendKeyDataResponse(int ProcessId, int SecretKey) : Response;
internal sealed record NotificationResponse(int ProcessId, string Channel, string Payload) : Response;
internal sealed record ParameterDescriptionResponse(int[] TypeOids) : Response;
internal sealed record ParameterStatusResponse(string Name, string Value) : Response;

internal sealed record ReadyForQueryResponse(char Status) : Response
{
    // Singleton instances for the 3 possible transaction states
    public static readonly ReadyForQueryResponse Idle = new('I');           // Not in a transaction
    public static readonly ReadyForQueryResponse InTransaction = new('T');  // In a transaction block
    public static readonly ReadyForQueryResponse Error = new('E');          // In a failed transaction block

    public static ReadyForQueryResponse Get(char status) => status switch
    {
        'I' => Idle,
        'T' => InTransaction,
        'E' => Error,
        _ => new ReadyForQueryResponse(status) // Fallback for unknown status
    };
}

internal sealed record RowDescriptionResponse(PgColumnDesc[] Columns) : Response;
