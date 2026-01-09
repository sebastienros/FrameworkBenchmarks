// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Vertx.PgClient.Codec;

/// <summary>
/// Buffer utilities for PostgreSQL protocol.
/// </summary>
internal static class BufferUtils
{
    private static readonly Encoding Utf8 = Encoding.UTF8;

    /// <summary>
    /// Reads a null-terminated UTF-8 string from the buffer.
    /// </summary>
    public static string ReadCString(ReadOnlySpan<byte> buffer, out int bytesRead)
    {
        var len = buffer.IndexOf((byte)0);
        
        if (len == -1)
        {
            throw new InvalidOperationException("Null terminator not found in buffer");
        }
        
        var result = Utf8.GetString(buffer[..len]);
        bytesRead = len + 1; // +1 for null terminator
        return result;
    }

    /// <summary>
    /// Writes a null-terminated UTF-8 string to the buffer.
    /// </summary>
    public static int WriteCStringUtf8(Span<byte> buffer, string s)
    {
        int bytesWritten = Utf8.GetBytes(s, buffer);
        buffer[bytesWritten] = 0;
        return bytesWritten + 1;
    }
}
