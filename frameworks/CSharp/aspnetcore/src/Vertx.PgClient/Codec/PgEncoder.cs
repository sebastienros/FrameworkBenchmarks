// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace Vertx.PgClient.Codec;

/// <summary>
/// Encodes PostgreSQL frontend messages.
/// This class is not thread-safe and should be used by a single connection.
/// </summary>
internal sealed class PgEncoder
{
    private static readonly byte[] BuffUser = "user"u8.ToArray();
    private static readonly byte[] BuffDatabase = "database"u8.ToArray();

    private byte[] _buffer = new byte[4096];
    private int _position;
    private int _statementCounter;

    public ReadOnlyMemory<byte> Buffer => new(_buffer, 0, _position);

    public void Reset() => _position = 0;

    private void EnsureCapacity(int additionalBytes)
    {
        if (_position + additionalBytes > _buffer.Length)
        {
            int newSize = Math.Max(_buffer.Length * 2, _position + additionalBytes);
            Array.Resize(ref _buffer, newSize);
        }
    }

    /// <summary>
    /// Generates a statement name into the provided buffer.
    /// The buffer must be at least 10 bytes (max: "S_FFFFFFFF").
    /// </summary>
    /// <param name="buffer">The buffer to write the statement name to.</param>
    /// <returns>The number of bytes written to the buffer.</returns>
    public int GenerateStatementName(Span<byte> buffer)
    {
        int counter = _statementCounter++;

        // Write "S_" prefix
        buffer[0] = (byte)'S';
        buffer[1] = (byte)'_';
        
        // Convert counter to hex and write directly to buffer
        int pos = 2;
        if (counter == 0)
        {
            buffer[pos++] = (byte)'0';
        }
        else
        {
            // Find the number of hex digits needed
            // The BitOperations.Log2 approach is the fastest since it compiles to a single CPU instruction (LZCNT or BSR)
            int hexDigits = (BitOperations.Log2((uint)counter) >> 2) + 1;
            
            const string HEX = "0123456789ABCDEF";

            for (int i = 0; i < hexDigits; i++)
            {
                buffer[pos++] = (byte)HEX[(counter >> (4 * (hexDigits - 1 - i))) & 0xF];
            }
        }

        return pos;
    }

    public void WriteStartupMessage(string username, string database, IReadOnlyDictionary<string, string> properties)
    {
        var startPos = _position;
        
        // Length placeholder
        WriteInt32(0);
        
        // Protocol version (3.0)
        WriteInt16(3);
        WriteInt16(0);

        // user
        WriteCString(BuffUser);
        WriteCStringUtf8(username);

        // database
        WriteCString(BuffDatabase);
        WriteCStringUtf8(database);

        // Properties
        foreach (var (key, value) in properties)
        {
            WriteCStringUtf8(key);
            WriteCStringUtf8(value);
        }

        // Terminator
        WriteByte(0);

        // Set length
        SetInt32(startPos, _position - startPos);
    }

    public void WritePasswordMessage(string password)
    {
        WriteByte(PgProtocolConstants.PasswordMessage);
        var lengthPos = _position;
        WriteInt32(0);
        WriteCStringUtf8(password);
        SetInt32(lengthPos, _position - lengthPos);
    }

    public void WriteMd5PasswordMessage(string hash)
    {
        WriteByte(PgProtocolConstants.PasswordMessage);
        var lengthPos = _position;
        WriteInt32(0);
        WriteCStringUtf8(hash);
        SetInt32(lengthPos, _position - lengthPos);
    }

    public void WriteQuery(string sql)
    {
        WriteByte(PgProtocolConstants.Query);
        var lengthPos = _position;
        WriteInt32(0);
        WriteCStringUtf8(sql);
        SetInt32(lengthPos, _position - lengthPos);
    }

    public void WriteParse(string sql, ReadOnlySpan<byte> statementName, int[]? parameterTypes = null)
    {
        WriteByte(PgProtocolConstants.Parse);
        var lengthPos = _position;
        WriteInt32(0);
        
        WriteCString(statementName);
        WriteCStringUtf8(sql);
        
        // Parameter types
        if (parameterTypes is null || parameterTypes.Length == 0)
        {
            WriteInt16(0);
        }
        else
        {
            WriteInt16((short)parameterTypes.Length);
            foreach (var typeOid in parameterTypes)
            {
                WriteInt32(typeOid);
            }
        }

        SetInt32(lengthPos, _position - lengthPos);
    }

    public void WriteBind(ReadOnlySpan<byte> statementName, string portal, Tuple? parameters, DataType[]? parameterTypes)
    {
        WriteByte(PgProtocolConstants.Bind);
        var lengthPos = _position;
        WriteInt32(0);

        // Portal name
        WriteCStringUtf8(portal);
        
        // Statement name  
        WriteCString(statementName);

        int paramCount = parameters?.Size ?? 0;

        // Parameter format codes (all binary)
        WriteInt16((short)paramCount);
        for (int i = 0; i < paramCount; i++)
        {
            WriteInt16(1); // Binary
        }

        // Parameter values
        WriteInt16((short)paramCount);
        for (int i = 0; i < paramCount; i++)
        {
            var value = parameters![i];
            if (value.IsNull)
            {
                WriteInt32(-1); // NULL
            }
            else
            {
                var dataType = parameterTypes is not null && i < parameterTypes.Length
                    ? parameterTypes[i]
                    : value.DataType;

                // Write value with length prefix
                var valueStart = _position;
                WriteInt32(0); // Length placeholder
                
                EnsureCapacity(1024);
                var span = _buffer.AsSpan(_position, 1024);
                int bytesWritten = value.EncodeBinary(span, dataType);
                _position += bytesWritten;
                
                SetInt32(valueStart, bytesWritten);
            }
        }

        // Result format codes (all binary for better performance)
        WriteInt16(1);
        WriteInt16(1); // Binary

        SetInt32(lengthPos, _position - lengthPos);
    }

    public void WriteDescribe(char type, ReadOnlySpan<byte> name = default)
    {
        WriteByte(PgProtocolConstants.Describe);
        var lengthPos = _position;
        WriteInt32(0);
        WriteByte((byte)type);
        if (name.Length > 0)
        {
            WriteCString(name);
        }
        else
        {
            WriteByte(0);
        }
        SetInt32(lengthPos, _position - lengthPos);
    }

    public void WriteExecute(string portal = "", int maxRows = 0)
    {
        WriteByte(PgProtocolConstants.Execute);
        var lengthPos = _position;
        WriteInt32(0);
        WriteCStringUtf8(portal);
        WriteInt32(maxRows);
        SetInt32(lengthPos, _position - lengthPos);
    }

    public void WriteClose(char type, ReadOnlySpan<byte> name = default)
    {
        WriteByte(PgProtocolConstants.Close);
        var lengthPos = _position;
        WriteInt32(0);
        WriteByte((byte)type);
        if (name.Length > 0)
        {
            WriteCString(name);
        }
        else
        {
            WriteByte(0);
        }
        SetInt32(lengthPos, _position - lengthPos);
    }

    public void WriteSync()
    {
        WriteByte(PgProtocolConstants.Sync);
        WriteInt32(4);
    }

    public void WriteTerminate()
    {
        WriteByte(PgProtocolConstants.Terminate);
        WriteInt32(4);
    }

    public void WriteSslRequest()
    {
        WriteInt32(8);
        WriteInt32(80877103); // SSL request code
    }

    /// <summary>
    /// Writes a SASLInitialResponse message for SCRAM authentication.
    /// </summary>
    public void WriteSaslInitialResponse(string mechanism, string clientFirstMessage)
    {
        var mechanismBytes = Encoding.UTF8.GetBytes(mechanism);
        var messageBytes = Encoding.UTF8.GetBytes(clientFirstMessage);

        WriteByte(PgProtocolConstants.PasswordMessage);
        var lengthPos = _position;
        WriteInt32(0);

        // Mechanism name (null-terminated)
        EnsureCapacity(mechanismBytes.Length + 1);
        mechanismBytes.CopyTo(_buffer.AsSpan(_position));
        _position += mechanismBytes.Length;
        _buffer[_position++] = 0;

        // Client first message length
        WriteInt32(messageBytes.Length);

        // Client first message data (not null-terminated)
        EnsureCapacity(messageBytes.Length);
        messageBytes.CopyTo(_buffer.AsSpan(_position));
        _position += messageBytes.Length;

        SetInt32(lengthPos, _position - lengthPos);
    }

    /// <summary>
    /// Writes a SASLResponse message for SCRAM authentication.
    /// </summary>
    public void WriteSaslResponse(string clientFinalMessage)
    {
        var messageBytes = Encoding.UTF8.GetBytes(clientFinalMessage);

        WriteByte(PgProtocolConstants.PasswordMessage);
        var lengthPos = _position;
        WriteInt32(0);

        // Message data (not null-terminated)
        EnsureCapacity(messageBytes.Length);
        messageBytes.CopyTo(_buffer.AsSpan(_position));
        _position += messageBytes.Length;

        SetInt32(lengthPos, _position - lengthPos);
    }

    private void WriteByte(byte value)
    {
        EnsureCapacity(1);
        _buffer[_position++] = value;
    }

    private void WriteInt16(short value)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteInt16BigEndian(_buffer.AsSpan(_position, 2), value);
        _position += 2;
    }

    private void WriteInt32(int value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(_position, 4), value);
        _position += 4;
    }

    private void SetInt32(int position, int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(position, 4), value);
    }

    private void WriteCString(ReadOnlySpan<byte> bytes)
    {
        EnsureCapacity(bytes.Length + 1);
        bytes.CopyTo(_buffer.AsSpan(_position));
        _position += bytes.Length;
        _buffer[_position++] = 0;
    }

    private void WriteCStringUtf8(string value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        EnsureCapacity(byteCount + 1);
        Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_position));
        _position += byteCount;
        _buffer[_position++] = 0;
    }
}
