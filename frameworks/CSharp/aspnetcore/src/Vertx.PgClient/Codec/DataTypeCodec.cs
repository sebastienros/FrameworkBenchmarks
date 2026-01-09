// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Buffers.Binary;
using System.Buffers.Text;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Vertx.PgClient.Data;

namespace Vertx.PgClient.Codec;

/// <summary>
/// Encodes and decodes PostgreSQL data types.
/// </summary>
public static class DataTypeCodec
{
    private static readonly DateOnly LocalDateEpoch = new(2000, 1, 1);
    private static readonly DateTime LocalDateTimeEpoch = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly Encoding Utf8 = Encoding.UTF8;

    #region Binary Decode

    /// <summary>
    /// Decodes a binary PostgreSQL value. Returns object, may box value types.
    /// For no-boxing decoding, use PgValue.DecodeBinary instead.
    /// </summary>
    internal static object? DecodeBinary(DataType dataType, ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty) return null;

        return dataType.Id switch
        {
            DataTypeId.Bool => DecodeBoolBinary(buffer),
            DataTypeId.Int2 => DecodeInt16Binary(buffer),
            DataTypeId.Int4 => DecodeInt32Binary(buffer),
            DataTypeId.Int8 => DecodeInt64Binary(buffer),
            DataTypeId.Float4 => DecodeFloatBinary(buffer),
            DataTypeId.Float8 => DecodeDoubleBinary(buffer),
            DataTypeId.Char or DataTypeId.Varchar or DataTypeId.Bpchar or DataTypeId.Text or DataTypeId.Name => DecodeStringBinary(buffer),
            DataTypeId.Date => DecodeDateBinary(buffer),
            DataTypeId.Time => DecodeTimeBinary(buffer),
            DataTypeId.Timetz => DecodeTimeTzBinary(buffer),
            DataTypeId.Timestamp => DecodeTimestampBinary(buffer),
            DataTypeId.Timestamptz => DecodeTimestampTzBinary(buffer),
            DataTypeId.Bytea => DecodeByteArrayBinary(buffer),
            DataTypeId.Uuid => DecodeGuidBinary(buffer),
            DataTypeId.Json or DataTypeId.Jsonb => DecodeJsonBinary(buffer),
            DataTypeId.Point => DecodePointBinary(buffer),
            DataTypeId.Line => DecodeLineBinary(buffer),
            DataTypeId.Lseg => DecodeLineSegmentBinary(buffer),
            DataTypeId.Box => DecodeBoxBinary(buffer),
            DataTypeId.Circle => DecodeCircleBinary(buffer),
            DataTypeId.Path => DecodePathBinary(buffer),
            DataTypeId.Polygon => DecodePolygonBinary(buffer),
            DataTypeId.Interval => DecodeIntervalBinary(buffer),
            DataTypeId.Inet => DecodeInetBinary(buffer),
            DataTypeId.Cidr => DecodeCidrBinary(buffer),
            DataTypeId.Money => DecodeMoneyBinary(buffer),
            DataTypeId.Numeric => DecodeNumericBinary(buffer),
            // Array types
            DataTypeId.BoolArray => DecodeBoolArrayBinary(buffer),
            DataTypeId.Int2Array => DecodeInt16ArrayBinary(buffer),
            DataTypeId.Int4Array => DecodeInt32ArrayBinary(buffer),
            DataTypeId.Int8Array => DecodeInt64ArrayBinary(buffer),
            DataTypeId.Float4Array => DecodeFloatArrayBinary(buffer),
            DataTypeId.Float8Array => DecodeDoubleArrayBinary(buffer),
            DataTypeId.NumericArray => DecodeDecimalArrayBinary(buffer),
            DataTypeId.VarcharArray or DataTypeId.TextArray or DataTypeId.BpcharArray or DataTypeId.NameArray => DecodeStringArrayBinary(buffer),
            DataTypeId.DateArray => DecodeDateArrayBinary(buffer),
            DataTypeId.TimestampArray => DecodeDateTimeArrayBinary(buffer),
            DataTypeId.TimestamptzArray => DecodeDateTimeOffsetArrayBinary(buffer),
            DataTypeId.UuidArray => DecodeGuidArrayBinary(buffer),
            DataTypeId.ByteaArray => DecodeByteArrayArrayBinary(buffer),
            _ => DecodeStringBinary(buffer) // Unknown types decode as string
        };
    }

    // Public typed decode methods for use by PgValue

    public static bool DecodeBoolBinary(ReadOnlySpan<byte> buffer) => buffer[0] != 0;

    public static short DecodeInt16Binary(ReadOnlySpan<byte> buffer) => BinaryPrimitives.ReadInt16BigEndian(buffer);

    public static int DecodeInt32Binary(ReadOnlySpan<byte> buffer) => BinaryPrimitives.ReadInt32BigEndian(buffer);

    public static long DecodeInt64Binary(ReadOnlySpan<byte> buffer) => BinaryPrimitives.ReadInt64BigEndian(buffer);

    public static float DecodeFloatBinary(ReadOnlySpan<byte> buffer)
    {
        int intBits = BinaryPrimitives.ReadInt32BigEndian(buffer);
        return BitConverter.Int32BitsToSingle(intBits);
    }

    public static double DecodeDoubleBinary(ReadOnlySpan<byte> buffer)
    {
        long longBits = BinaryPrimitives.ReadInt64BigEndian(buffer);
        return BitConverter.Int64BitsToDouble(longBits);
    }

    public static string DecodeStringBinary(ReadOnlySpan<byte> buffer) => Utf8.GetString(buffer);

    public static DateOnly DecodeDateBinary(ReadOnlySpan<byte> buffer)
    {
        int days = BinaryPrimitives.ReadInt32BigEndian(buffer);
        return LocalDateEpoch.AddDays(days);
    }

    public static TimeOnly DecodeTimeBinary(ReadOnlySpan<byte> buffer)
    {
        long micros = BinaryPrimitives.ReadInt64BigEndian(buffer);
        return TimeOnly.FromTimeSpan(TimeSpan.FromTicks(micros * 10));
    }

    public static DateTimeOffset DecodeTimeTzBinary(ReadOnlySpan<byte> buffer)
    {
        long micros = BinaryPrimitives.ReadInt64BigEndian(buffer);
        int offsetSeconds = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(8));
        var time = TimeSpan.FromTicks(micros * 10);
        // PostgreSQL stores offset as seconds from UTC (negated)
        var offset = TimeSpan.FromSeconds(-offsetSeconds);
        return new DateTimeOffset(DateTime.Today.Add(time), offset);
    }

    public static DateTime DecodeTimestampBinary(ReadOnlySpan<byte> buffer)
    {
        long micros = BinaryPrimitives.ReadInt64BigEndian(buffer);
        return LocalDateTimeEpoch.AddTicks(micros * 10);
    }

    public static DateTimeOffset DecodeTimestampTzBinary(ReadOnlySpan<byte> buffer)
    {
        long micros = BinaryPrimitives.ReadInt64BigEndian(buffer);
        return new DateTimeOffset(LocalDateTimeEpoch.AddTicks(micros * 10), TimeSpan.Zero);
    }

    public static byte[] DecodeByteArrayBinary(ReadOnlySpan<byte> buffer) => buffer.ToArray();

    public static Guid DecodeGuidBinary(ReadOnlySpan<byte> buffer)
    {
        // PostgreSQL sends UUID in network byte order (big-endian)
        // .NET Guid expects mixed-endian format
        Span<byte> bytes = stackalloc byte[16];
        buffer.CopyTo(bytes);
        
        // Swap bytes for the first three components (little-endian in .NET)
        // time_low (4 bytes)
        (bytes[0], bytes[3]) = (bytes[3], bytes[0]);
        (bytes[1], bytes[2]) = (bytes[2], bytes[1]);
        // time_mid (2 bytes)
        (bytes[4], bytes[5]) = (bytes[5], bytes[4]);
        // time_hi_and_version (2 bytes)
        (bytes[6], bytes[7]) = (bytes[7], bytes[6]);
        
        return new Guid(bytes);
    }

    public static string DecodeJsonBinary(ReadOnlySpan<byte> buffer)
    {
        // JSONB has a version byte prefix
        if (buffer.Length > 0 && buffer[0] == 1)
        {
            return Utf8.GetString(buffer.Slice(1));
        }
        return Utf8.GetString(buffer);
    }

    public static Point DecodePointBinary(ReadOnlySpan<byte> buffer)
    {
        double x = DecodeDoubleBinary(buffer);
        double y = DecodeDoubleBinary(buffer.Slice(8));
        return new Point(x, y);
    }

    public static Line DecodeLineBinary(ReadOnlySpan<byte> buffer)
    {
        double a = DecodeDoubleBinary(buffer);
        double b = DecodeDoubleBinary(buffer.Slice(8));
        double c = DecodeDoubleBinary(buffer.Slice(16));
        return new Line(a, b, c);
    }

    public static LineSegment DecodeLineSegmentBinary(ReadOnlySpan<byte> buffer)
    {
        var p1 = DecodePointBinary(buffer);
        var p2 = DecodePointBinary(buffer.Slice(16));
        return new LineSegment(p1, p2);
    }

    public static Box DecodeBoxBinary(ReadOnlySpan<byte> buffer)
    {
        var upperRight = DecodePointBinary(buffer);
        var lowerLeft = DecodePointBinary(buffer.Slice(16));
        return new Box(upperRight, lowerLeft);
    }

    public static Circle DecodeCircleBinary(ReadOnlySpan<byte> buffer)
    {
        var center = DecodePointBinary(buffer);
        double radius = DecodeDoubleBinary(buffer.Slice(16));
        return new Circle(center, radius);
    }

    public static Interval DecodeIntervalBinary(ReadOnlySpan<byte> buffer)
    {
        long micros = BinaryPrimitives.ReadInt64BigEndian(buffer);
        int days = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(8));
        int months = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(12));

        int years = months / 12;
        months %= 12;
        
        int seconds = (int)(micros / 1_000_000);
        int microseconds = (int)(micros % 1_000_000);
        int hours = seconds / 3600;
        seconds %= 3600;
        int minutes = seconds / 60;
        seconds %= 60;

        return new Interval(years, months, days, hours, minutes, seconds, microseconds);
    }

    public static Inet DecodeInetBinary(ReadOnlySpan<byte> buffer)
    {
        // Format: family (1 byte), netmask (1 byte), is_cidr (1 byte), address length (1 byte), address
        byte family = buffer[0];
        byte netmask = buffer[1];
        // byte isCidr = buffer[2];
        byte addrLen = buffer[3];
        
        var addressBytes = buffer.Slice(4, addrLen);
        var address = new IPAddress(addressBytes);
        
        return new Inet().SetAddress(address).SetNetmask(netmask);
    }

    public static Cidr DecodeCidrBinary(ReadOnlySpan<byte> buffer)
    {
        // Format: family (1 byte), netmask (1 byte), is_cidr (1 byte), address length (1 byte), address
        byte family = buffer[0];
        byte netmask = buffer[1];
        // byte isCidr = buffer[2];
        byte addrLen = buffer[3];
        
        var addressBytes = buffer.Slice(4, addrLen);
        var address = new IPAddress(addressBytes);
        
        return new Cidr().SetAddress(address).SetNetmask(netmask);
    }

    public static Data.Path DecodePathBinary(ReadOnlySpan<byte> buffer)
    {
        // Format: closed flag (1 byte), point count (4 bytes), points (16 bytes each)
        bool isOpen = buffer[0] == 0;
        int pointCount = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(1));
        var points = new List<Point>(pointCount);
        int offset = 5;
        for (int i = 0; i < pointCount; i++)
        {
            points.Add(DecodePointBinary(buffer.Slice(offset)));
            offset += 16;
        }
        return new Data.Path(isOpen, points);
    }

    public static Polygon DecodePolygonBinary(ReadOnlySpan<byte> buffer)
    {
        // Format: point count (4 bytes), points (16 bytes each)
        int pointCount = BinaryPrimitives.ReadInt32BigEndian(buffer);
        var points = new List<Point>(pointCount);
        int offset = 4;
        for (int i = 0; i < pointCount; i++)
        {
            points.Add(DecodePointBinary(buffer.Slice(offset)));
            offset += 16;
        }
        return new Polygon(points);
    }

    public static Money DecodeMoneyBinary(ReadOnlySpan<byte> buffer)
    {
        long cents = BinaryPrimitives.ReadInt64BigEndian(buffer);
        return new Money(cents / 100m);
    }

    public static decimal DecodeNumericBinary(ReadOnlySpan<byte> buffer)
    {
        // PostgreSQL numeric format:
        // ndigits (2), weight (2), sign (2), dscale (2), digits (2 * ndigits)
        int ndigits = BinaryPrimitives.ReadInt16BigEndian(buffer);
        int weight = BinaryPrimitives.ReadInt16BigEndian(buffer.Slice(2));
        int sign = BinaryPrimitives.ReadInt16BigEndian(buffer.Slice(4));
        int dscale = BinaryPrimitives.ReadInt16BigEndian(buffer.Slice(6));

        if (ndigits == 0)
        {
            return 0m;
        }

        // Each digit is base-10000
        const int nbase = 10000;
        decimal result = 0;
        int offset = 8;
        
        for (int i = 0; i < ndigits; i++)
        {
            short digit = BinaryPrimitives.ReadInt16BigEndian(buffer.Slice(offset));
            result = result * nbase + digit;
            offset += 2;
        }

        // Apply weight (each unit of weight = 4 decimal digits)
        int exponent = (weight + 1 - ndigits) * 4;
        if (exponent > 0)
        {
            for (int i = 0; i < exponent; i++)
            {
                result *= 10;
            }
        }
        else if (exponent < 0)
        {
            for (int i = 0; i < -exponent; i++)
            {
                result /= 10;
            }
        }

        // Apply sign (0 = positive, 0x4000 = negative, 0xC000 = NaN)
        if (sign == 0x4000)
        {
            result = -result;
        }

        return result;
    }

    /// <summary>
    /// Decodes a PostgreSQL array in binary format.
    /// Binary array format:
    /// - ndim (4 bytes): number of dimensions
    /// - hasNull (4 bytes): 1 if array contains NULLs
    /// - elemType (4 bytes): OID of element type
    /// - For each dimension: dim (4 bytes), lbound (4 bytes)
    /// - For each element: length (4 bytes, -1 for NULL), data (length bytes)
    /// </summary>
    private static T?[] DecodeArrayBinaryGeneric<T>(ReadOnlySpan<byte> buffer, DataType elementType)
    {
        if (buffer.Length < 12)
        {
            return Array.Empty<T?>();
        }

        int ndim = BinaryPrimitives.ReadInt32BigEndian(buffer);
        int hasNull = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(4));
        int elemTypeOid = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(8));
        
        if (ndim == 0)
        {
            return Array.Empty<T?>();
        }

        // For now, only support 1-dimensional arrays
        if (ndim != 1)
        {
            throw new NotSupportedException($"Multi-dimensional arrays (ndim={ndim}) are not yet supported");
        }

        int offset = 12;
        
        // Read dimension info
        int dim = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(offset));
        int lbound = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(offset + 4));
        offset += 8;

        var result = new T?[dim];
        
        for (int i = 0; i < dim; i++)
        {
            int elemLen = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(offset));
            offset += 4;
            
            if (elemLen == -1)
            {
                // NULL element
                result[i] = default;
            }
            else
            {
                var elemData = buffer.Slice(offset, elemLen);
                var decoded = DecodeBinary(elementType, elemData);
                result[i] = decoded is T typedValue ? typedValue : default;
                offset += elemLen;
            }
        }
        
        return result;
    }

    // Public typed array decode methods

    public static bool[]? DecodeBoolArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<bool>(buffer, DataType.Bool)!;

    public static short[]? DecodeInt16ArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<short>(buffer, DataType.Int2)!;

    public static int[]? DecodeInt32ArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<int>(buffer, DataType.Int4)!;

    public static long[]? DecodeInt64ArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<long>(buffer, DataType.Int8)!;

    public static float[]? DecodeFloatArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<float>(buffer, DataType.Float4)!;

    public static double[]? DecodeDoubleArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<double>(buffer, DataType.Float8)!;

    public static decimal[]? DecodeDecimalArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<decimal>(buffer, DataType.Numeric)!;

    public static string?[]? DecodeStringArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<string>(buffer, DataType.Text);

    public static DateOnly[]? DecodeDateArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<DateOnly>(buffer, DataType.Date)!;

    public static DateTime[]? DecodeDateTimeArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<DateTime>(buffer, DataType.Timestamp)!;

    public static DateTimeOffset[]? DecodeDateTimeOffsetArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<DateTimeOffset>(buffer, DataType.Timestamptz)!;

    public static Guid[]? DecodeGuidArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<Guid>(buffer, DataType.Uuid)!;

    public static byte[]?[]? DecodeByteArrayArrayBinary(ReadOnlySpan<byte> buffer)
        => DecodeArrayBinaryGeneric<byte[]>(buffer, DataType.Bytea);

    #endregion

    #region Binary Encode

    /// <summary>
    /// Encodes a value to binary PostgreSQL format. May cause boxing.
    /// For no-boxing encoding, use PgValue.EncodeBinary instead.
    /// </summary>
    internal static void EncodeBinary(DataType dataType, object? value, Span<byte> buffer, out int bytesWritten)
    {
        if (value is null)
        {
            bytesWritten = 0;
            return;
        }

        bytesWritten = dataType.Id switch
        {
            DataTypeId.Bool => EncodeBoolBinary((bool)value, buffer),
            DataTypeId.Int2 => EncodeInt16Binary(Convert.ToInt16(value), buffer),
            DataTypeId.Int4 => EncodeInt32Binary(Convert.ToInt32(value), buffer),
            DataTypeId.Int8 => EncodeInt64Binary(Convert.ToInt64(value), buffer),
            DataTypeId.Float4 => EncodeFloatBinary(Convert.ToSingle(value), buffer),
            DataTypeId.Float8 => EncodeDoubleBinary(Convert.ToDouble(value), buffer),
            DataTypeId.Numeric => EncodeNumericBinary(Convert.ToDecimal(value), buffer),
            DataTypeId.Char or DataTypeId.Varchar or DataTypeId.Bpchar or DataTypeId.Text or DataTypeId.Name => 
                EncodeStringBinary((string)value, buffer),
            DataTypeId.Date => EncodeDateBinary((DateOnly)value, buffer),
            DataTypeId.Time => EncodeTimeBinary((TimeOnly)value, buffer),
            DataTypeId.Timestamp => EncodeTimestampBinary((DateTime)value, buffer),
            DataTypeId.Timestamptz => EncodeTimestampTzBinary((DateTimeOffset)value, buffer),
            DataTypeId.Bytea => EncodeByteArrayBinary((byte[])value, buffer),
            DataTypeId.Uuid => EncodeGuidBinary((Guid)value, buffer),
            DataTypeId.Json => EncodeStringBinary((string)value, buffer),
            DataTypeId.Jsonb => EncodeJsonbBinary((string)value, buffer),
            DataTypeId.Point => EncodePointBinary((Point)value, buffer),
            DataTypeId.Line => EncodeLineBinary((Line)value, buffer),
            DataTypeId.Lseg => EncodeLineSegmentBinary((LineSegment)value, buffer),
            DataTypeId.Box => EncodeBoxBinary((Box)value, buffer),
            DataTypeId.Circle => EncodeCircleBinary((Circle)value, buffer),
            DataTypeId.Path => EncodePathBinary((Data.Path)value, buffer),
            DataTypeId.Polygon => EncodePolygonBinary((Polygon)value, buffer),
            DataTypeId.Inet => EncodeInetBinary((Inet)value, buffer),
            DataTypeId.Cidr => EncodeCidrBinary((Cidr)value, buffer),
            DataTypeId.Interval => EncodeIntervalBinary((Interval)value, buffer),
            // Array types
            DataTypeId.BoolArray => EncodeBoolArrayBinary((bool[])value, buffer),
            DataTypeId.Int2Array => EncodeInt16ArrayBinary((short[])value, buffer),
            DataTypeId.Int4Array => EncodeInt32ArrayBinary((int[])value, buffer),
            DataTypeId.Int8Array => EncodeInt64ArrayBinary((long[])value, buffer),
            DataTypeId.Float4Array => EncodeFloatArrayBinary((float[])value, buffer),
            DataTypeId.Float8Array => EncodeDoubleArrayBinary((double[])value, buffer),
            DataTypeId.VarcharArray or DataTypeId.TextArray => EncodeStringArrayBinary((string[])value, buffer),
            DataTypeId.DateArray => EncodeDateArrayBinary((DateOnly[])value, buffer),
            DataTypeId.TimestampArray => EncodeDateTimeArrayBinary((DateTime[])value, buffer),
            DataTypeId.TimestamptzArray => EncodeDateTimeOffsetArrayBinary((DateTimeOffset[])value, buffer),
            DataTypeId.UuidArray => EncodeGuidArrayBinary((Guid[])value, buffer),
            _ => EncodeStringBinary(value.ToString() ?? "", buffer)
        };
    }

    // Public typed encode methods for use by PgValue

    public static int EncodeBoolBinary(bool value, Span<byte> buffer)
    {
        buffer[0] = value ? (byte)1 : (byte)0;
        return 1;
    }

    public static int EncodeInt16Binary(short value, Span<byte> buffer)
    {
        BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        return 2;
    }

    public static int EncodeInt32Binary(int value, Span<byte> buffer)
    {
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        return 4;
    }

    public static int EncodeInt64Binary(long value, Span<byte> buffer)
    {
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        return 8;
    }

    public static int EncodeFloatBinary(float value, Span<byte> buffer)
    {
        int intBits = BitConverter.SingleToInt32Bits(value);
        BinaryPrimitives.WriteInt32BigEndian(buffer, intBits);
        return 4;
    }

    public static int EncodeDoubleBinary(double value, Span<byte> buffer)
    {
        long longBits = BitConverter.DoubleToInt64Bits(value);
        BinaryPrimitives.WriteInt64BigEndian(buffer, longBits);
        return 8;
    }

    public static int EncodeStringBinary(string value, Span<byte> buffer)
    {
        return Utf8.GetBytes(value, buffer);
    }

    public static int EncodeJsonbBinary(string value, Span<byte> buffer)
    {
        // JSONB binary format requires a version byte prefix (always 1)
        buffer[0] = 1;
        return 1 + Utf8.GetBytes(value, buffer.Slice(1));
    }

    public static int EncodeDateBinary(DateOnly value, Span<byte> buffer)
    {
        int days = value.DayNumber - LocalDateEpoch.DayNumber;
        BinaryPrimitives.WriteInt32BigEndian(buffer, days);
        return 4;
    }

    public static int EncodeTimeBinary(TimeOnly value, Span<byte> buffer)
    {
        long micros = value.Ticks / 10;
        BinaryPrimitives.WriteInt64BigEndian(buffer, micros);
        return 8;
    }

    public static int EncodeTimestampBinary(DateTime value, Span<byte> buffer)
    {
        long micros = (value.Ticks - LocalDateTimeEpoch.Ticks) / 10;
        BinaryPrimitives.WriteInt64BigEndian(buffer, micros);
        return 8;
    }

    public static int EncodeTimestampTzBinary(DateTimeOffset value, Span<byte> buffer)
    {
        var utc = value.UtcDateTime;
        long micros = (utc.Ticks - LocalDateTimeEpoch.Ticks) / 10;
        BinaryPrimitives.WriteInt64BigEndian(buffer, micros);
        return 8;
    }

    public static int EncodeByteArrayBinary(byte[] value, Span<byte> buffer)
    {
        value.CopyTo(buffer);
        return value.Length;
    }

    public static int EncodeGuidBinary(Guid value, Span<byte> buffer)
    {
        value.TryWriteBytes(buffer);
        
        // Swap bytes back to network byte order (big-endian)
        (buffer[0], buffer[3]) = (buffer[3], buffer[0]);
        (buffer[1], buffer[2]) = (buffer[2], buffer[1]);
        (buffer[4], buffer[5]) = (buffer[5], buffer[4]);
        (buffer[6], buffer[7]) = (buffer[7], buffer[6]);
        
        return 16;
    }

    public static int EncodePointBinary(Point value, Span<byte> buffer)
    {
        int written = EncodeDoubleBinary(value.X, buffer);
        written += EncodeDoubleBinary(value.Y, buffer.Slice(8));
        return written;
    }

    public static int EncodeIntervalBinary(Interval value, Span<byte> buffer)
    {
        long micros = (long)value.Hours * 3600_000_000L +
                     (long)value.Minutes * 60_000_000L +
                     (long)value.Seconds * 1_000_000L +
                     value.Microseconds;
        
        int months = value.Years * 12 + value.Months;
        
        BinaryPrimitives.WriteInt64BigEndian(buffer, micros);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(8), value.Days);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(12), months);
        
        return 16;
    }

    public static int EncodeLineBinary(Line value, Span<byte> buffer)
    {
        int written = EncodeDoubleBinary(value.A, buffer);
        written += EncodeDoubleBinary(value.B, buffer.Slice(8));
        written += EncodeDoubleBinary(value.C, buffer.Slice(16));
        return written;
    }

    public static int EncodeLineSegmentBinary(LineSegment value, Span<byte> buffer)
    {
        int written = EncodePointBinary(value.P1, buffer);
        written += EncodePointBinary(value.P2, buffer.Slice(16));
        return written;
    }

    public static int EncodeBoxBinary(Box value, Span<byte> buffer)
    {
        int written = EncodePointBinary(value.UpperRightCorner, buffer);
        written += EncodePointBinary(value.LowerLeftCorner, buffer.Slice(16));
        return written;
    }

    public static int EncodeCircleBinary(Circle value, Span<byte> buffer)
    {
        int written = EncodePointBinary(value.CenterPoint, buffer);
        written += EncodeDoubleBinary(value.Radius, buffer.Slice(16));
        return written;
    }

    public static int EncodePathBinary(Data.Path value, Span<byte> buffer)
    {
        // Format: closed flag (1 byte), point count (4 bytes), points (16 bytes each)
        buffer[0] = value.IsOpen ? (byte)0 : (byte)1;
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(1), value.Points.Count);
        int offset = 5;
        foreach (var point in value.Points)
        {
            EncodePointBinary(point, buffer.Slice(offset));
            offset += 16;
        }
        return offset;
    }

    public static int EncodePolygonBinary(Polygon value, Span<byte> buffer)
    {
        // Format: point count (4 bytes), points (16 bytes each)
        BinaryPrimitives.WriteInt32BigEndian(buffer, value.Points.Count);
        int offset = 4;
        foreach (var point in value.Points)
        {
            EncodePointBinary(point, buffer.Slice(offset));
            offset += 16;
        }
        return offset;
    }

    public static int EncodeInetBinary(Inet value, Span<byte> buffer)
    {
        // Format: family (1 byte), netmask (1 byte), is_cidr (1 byte), address length (1 byte), address
        var addressBytes = value.Address!.GetAddressBytes();
        bool isIPv6 = value.Address.AddressFamily == AddressFamily.InterNetworkV6;
        
        buffer[0] = isIPv6 ? (byte)3 : (byte)2; // family: 2=IPv4, 3=IPv6
        buffer[1] = (byte)(value.Netmask ?? (isIPv6 ? 128 : 32));
        buffer[2] = 0; // is_cidr = false for inet
        buffer[3] = (byte)addressBytes.Length;
        addressBytes.CopyTo(buffer.Slice(4));
        
        return 4 + addressBytes.Length;
    }

    public static int EncodeCidrBinary(Cidr value, Span<byte> buffer)
    {
        // Format: family (1 byte), netmask (1 byte), is_cidr (1 byte), address length (1 byte), address
        var addressBytes = value.Address!.GetAddressBytes();
        bool isIPv6 = value.Address.AddressFamily == AddressFamily.InterNetworkV6;
        
        buffer[0] = isIPv6 ? (byte)3 : (byte)2; // family: 2=IPv4, 3=IPv6
        buffer[1] = (byte)(value.Netmask ?? (isIPv6 ? 128 : 32));
        buffer[2] = 1; // is_cidr = true for cidr
        buffer[3] = (byte)addressBytes.Length;
        addressBytes.CopyTo(buffer.Slice(4));
        
        return 4 + addressBytes.Length;
    }

    public static int EncodeNumericBinary(decimal value, Span<byte> buffer)
    {
        // For simplicity, use text encoding for numeric since binary format is complex
        // This converts to string and encodes as UTF-8
        var str = value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return Utf8.GetBytes(str, buffer);
    }

    // Public typed array encode methods - optimized to avoid intermediate nullable array allocations

    public static int EncodeBoolArrayBinary(bool[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Bool, buffer, static (v, b) => EncodeBoolBinary(v, b));

    public static int EncodeInt16ArrayBinary(short[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Int2, buffer, static (v, b) => EncodeInt16Binary(v, b));

    public static int EncodeInt32ArrayBinary(int[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Int4, buffer, static (v, b) => EncodeInt32Binary(v, b));

    public static int EncodeInt64ArrayBinary(long[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Int8, buffer, static (v, b) => EncodeInt64Binary(v, b));

    public static int EncodeFloatArrayBinary(float[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Float4, buffer, static (v, b) => EncodeFloatBinary(v, b));

    public static int EncodeDoubleArrayBinary(double[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Float8, buffer, static (v, b) => EncodeDoubleBinary(v, b));

    public static int EncodeStringArrayBinary(string?[] array, Span<byte> buffer)
        => EncodeArrayBinaryGeneric(array, DataType.Text, buffer);

    public static int EncodeDateArrayBinary(DateOnly[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Date, buffer, static (v, b) => EncodeDateBinary(v, b));

    public static int EncodeDateTimeArrayBinary(DateTime[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Timestamp, buffer, static (v, b) => EncodeTimestampBinary(v, b));

    public static int EncodeDateTimeOffsetArrayBinary(DateTimeOffset[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Timestamptz, buffer, static (v, b) => EncodeTimestampTzBinary(v, b));

    public static int EncodeGuidArrayBinary(Guid[] array, Span<byte> buffer)
        => EncodeValueTypeArrayBinary(array, DataType.Uuid, buffer, static (v, b) => EncodeGuidBinary(v, b));

    /// <summary>
    /// Encodes a value type array directly without creating an intermediate nullable array.
    /// </summary>
    private static int EncodeValueTypeArrayBinary<T>(T[] array, DataType elementType, Span<byte> buffer, Func<T, Span<byte>, int> encoder) where T : struct
    {
        int offset = 0;
        
        // ndim = 1
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), 1);
        offset += 4;
        
        // hasNull = 0 (value type arrays cannot have null elements)
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), 0);
        offset += 4;
        
        // elemType OID
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), (int)elementType.Id);
        offset += 4;
        
        // dim = array length
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), array.Length);
        offset += 4;
        
        // lbound = 1 (PostgreSQL arrays are 1-indexed by default)
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), 1);
        offset += 4;
        
        // Elements - no null checks needed for value types
        foreach (var elem in array)
        {
            // Reserve space for length, encode element, then write length
            int lengthOffset = offset;
            offset += 4;
            
            int elemBytes = encoder(elem, buffer.Slice(offset));
            BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(lengthOffset), elemBytes);
            offset += elemBytes;
        }
        
        return offset;
    }

    /// <summary>
    /// Encodes a .NET array as PostgreSQL binary array format.
    /// Binary array format:
    /// - ndim (4 bytes): number of dimensions (always 1 for now)
    /// - hasNull (4 bytes): 1 if array contains NULLs
    /// - elemType (4 bytes): OID of element type
    /// - dim (4 bytes): size of dimension
    /// - lbound (4 bytes): lower bound (always 1)
    /// - For each element: length (4 bytes, -1 for NULL), data (length bytes)
    /// </summary>
    private static int EncodeArrayBinaryGeneric<T>(T?[] array, DataType elementType, Span<byte> buffer)
    {
        int offset = 0;
        
        // ndim = 1
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), 1);
        offset += 4;
        
        // hasNull - check if any element is null
        bool hasNull = array.Any(x => x is null);
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), hasNull ? 1 : 0);
        offset += 4;
        
        // elemType OID
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), (int)elementType.Id);
        offset += 4;
        
        // dim = array length
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), array.Length);
        offset += 4;
        
        // lbound = 1 (PostgreSQL arrays are 1-indexed by default)
        BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), 1);
        offset += 4;
        
        // Elements
        foreach (var elem in array)
        {
            if (elem is null)
            {
                // NULL marker
                BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset), -1);
                offset += 4;
            }
            else
            {
                // Reserve space for length, encode element, then write length
                int lengthOffset = offset;
                offset += 4;
                
                EncodeBinary(elementType, elem, buffer.Slice(offset), out int elemBytes);
                BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(lengthOffset), elemBytes);
                offset += elemBytes;
            }
        }
        
        return offset;
    }

    #endregion

    #region Text Decode

    /// <summary>
    /// Decodes a text PostgreSQL value. Returns object, may box value types.
    /// For no-boxing decoding, use PgValue.DecodeText instead.
    /// </summary>
    internal static object? DecodeText(DataType dataType, ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty)
        {
            return null;
        }

        return dataType.Id switch
        {
            DataTypeId.Bool => buffer[0] == 't' || buffer[0] == '1',
            DataTypeId.Int2 => DecodeInt16Text(buffer),
            DataTypeId.Int4 => DecodeInt32Text(buffer),
            DataTypeId.Int8 => DecodeInt64Text(buffer),
            DataTypeId.Float4 => DecodeFloatText(buffer),
            DataTypeId.Float8 => DecodeDoubleText(buffer),
            DataTypeId.Numeric => DecodeDecimalText(buffer),
            DataTypeId.Char or DataTypeId.Varchar or DataTypeId.Bpchar or DataTypeId.Text or DataTypeId.Name => Utf8.GetString(buffer),
            DataTypeId.Date => DecodeDateText(buffer),
            DataTypeId.Time => DecodeTimeText(buffer),
            DataTypeId.Timestamp => DecodeDateTimeText(buffer),
            DataTypeId.Timestamptz => DecodeDateTimeOffsetText(buffer),
            DataTypeId.Uuid => DecodeGuidText(buffer),
            DataTypeId.Json or DataTypeId.Jsonb => Utf8.GetString(buffer),
            DataTypeId.Bytea => DecodeByteaText(buffer),
            DataTypeId.Point => DecodePointText(buffer),
            DataTypeId.Line => DecodeLineText(buffer),
            DataTypeId.Lseg => DecodeLsegText(buffer),
            DataTypeId.Box => DecodeBoxText(buffer),
            DataTypeId.Path => DecodePathText(buffer),
            DataTypeId.Polygon => DecodePolygonText(buffer),
            DataTypeId.Circle => DecodeCircleText(buffer),
            DataTypeId.Inet => DecodeInetText(buffer),
            DataTypeId.Cidr => DecodeCidrText(buffer),
            DataTypeId.Interval => DecodeIntervalText(buffer),
            DataTypeId.BoolArray => DecodeBoolArrayText(buffer),
            DataTypeId.Int2Array => DecodeInt2ArrayText(buffer),
            DataTypeId.Int4Array => DecodeInt4ArrayText(buffer),
            DataTypeId.Int8Array => DecodeInt8ArrayText(buffer),
            DataTypeId.Float4Array => DecodeFloat4ArrayText(buffer),
            DataTypeId.Float8Array => DecodeFloat8ArrayText(buffer),
            DataTypeId.VarcharArray or DataTypeId.TextArray or DataTypeId.BpcharArray or DataTypeId.NameArray => DecodeTextArrayText(buffer),
            DataTypeId.DateArray => DecodeDateArrayText(buffer),
            DataTypeId.TimestampArray => DecodeTimestampArrayText(buffer),
            DataTypeId.TimestamptzArray => DecodeTimestamptzArrayText(buffer),
            DataTypeId.UuidArray => DecodeUuidArrayText(buffer),
            _ => Utf8.GetString(buffer)
        };
    }

    // Public typed text decode methods

    public static short DecodeInt16Text(ReadOnlySpan<byte> buffer)
    {
        if (System.Buffers.Text.Utf8Parser.TryParse(buffer, out short value, out _))
        {
            return value;
        }
        throw new FormatException("Invalid Int16 format");
    }

    public static int DecodeInt32Text(ReadOnlySpan<byte> buffer)
    {
        if (System.Buffers.Text.Utf8Parser.TryParse(buffer, out int value, out _))
        {
            return value;
        }
        throw new FormatException("Invalid Int32 format");
    }

    public static long DecodeInt64Text(ReadOnlySpan<byte> buffer)
    {
        if (System.Buffers.Text.Utf8Parser.TryParse(buffer, out long value, out _))
        {
            return value;
        }
        throw new FormatException("Invalid Int64 format");
    }

    public static float DecodeFloatText(ReadOnlySpan<byte> buffer)
    {
        if (System.Buffers.Text.Utf8Parser.TryParse(buffer, out float value, out _))
        {
            return value;
        }
        throw new FormatException("Invalid Single format");
    }

    public static double DecodeDoubleText(ReadOnlySpan<byte> buffer)
    {
        if (System.Buffers.Text.Utf8Parser.TryParse(buffer, out double value, out _))
        {
            return value;
        }
        throw new FormatException("Invalid Double format");
    }

    public static decimal DecodeDecimalText(ReadOnlySpan<byte> buffer)
    {
        if (System.Buffers.Text.Utf8Parser.TryParse(buffer, out decimal value, out _))
        {
            return value;
        }
        throw new FormatException("Invalid Decimal format");
    }

    public static Guid DecodeGuidText(ReadOnlySpan<byte> buffer)
    {
        if (System.Buffers.Text.Utf8Parser.TryParse(buffer, out Guid value, out _))
        {
            return value;
        }
        throw new FormatException("Invalid Guid format");
    }

    /// <summary>
    /// Converts ASCII UTF-8 bytes to chars without allocation using stackalloc.
    /// PostgreSQL date/time formats are always ASCII, so this is safe.
    /// </summary>
    private static ReadOnlySpan<char> Utf8AsciiToChars(ReadOnlySpan<byte> buffer, Span<char> destination)
    {
        for (int i = 0; i < buffer.Length; i++)
        {
            destination[i] = (char)buffer[i];
        }
        return destination.Slice(0, buffer.Length);
    }

    public static DateOnly DecodeDateText(ReadOnlySpan<byte> buffer)
    {
        // PostgreSQL date format is ASCII (e.g., "2023-01-15")
        // Max length for date is ~10 chars, use 32 for safety
        Span<char> chars = stackalloc char[Math.Min(buffer.Length, 32)];
        return DateOnly.Parse(Utf8AsciiToChars(buffer, chars));
    }

    public static TimeOnly DecodeTimeText(ReadOnlySpan<byte> buffer)
    {
        // PostgreSQL time format is ASCII (e.g., "12:30:45.123456")
        // Max length for time is ~15 chars, use 32 for safety
        Span<char> chars = stackalloc char[Math.Min(buffer.Length, 32)];
        return TimeOnly.Parse(Utf8AsciiToChars(buffer, chars));
    }

    public static DateTime DecodeDateTimeText(ReadOnlySpan<byte> buffer)
    {
        // PostgreSQL timestamp format is ASCII (e.g., "2023-01-15 12:30:45.123456")
        // Max length is ~26 chars, use 64 for safety
        Span<char> chars = stackalloc char[Math.Min(buffer.Length, 64)];
        return DateTime.Parse(Utf8AsciiToChars(buffer, chars));
    }

    public static DateTimeOffset DecodeDateTimeOffsetText(ReadOnlySpan<byte> buffer)
    {
        // PostgreSQL timestamptz format is ASCII (e.g., "2023-01-15 12:30:45.123456+00")
        // Max length is ~32 chars, use 64 for safety
        Span<char> chars = stackalloc char[Math.Min(buffer.Length, 64)];
        return DateTimeOffset.Parse(Utf8AsciiToChars(buffer, chars));
    }

    public static byte[] DecodeByteaText(ReadOnlySpan<byte> buffer)
    {
        // PostgreSQL bytea text format is hex: \x48656c6c6f
        if (buffer.Length >= 2 && buffer[0] == '\\' && buffer[1] == 'x')
        {
            // Convert hex portion to string and parse
            return Convert.FromHexString(Utf8.GetString(buffer[2..]));
        }
        
        // Fallback for escape format (legacy)
        return buffer.ToArray();
    }

    public static Point DecodePointText(ReadOnlySpan<byte> buffer)
    {
        // Format: (x,y)
        if (buffer.Length >= 5 && buffer[0] == '(' && buffer[^1] == ')')
        {
            var inner = buffer[1..^1]; // Remove parentheses
            int commaIndex = inner.IndexOf((byte)',');
            if (commaIndex > 0)
            {
                if (System.Buffers.Text.Utf8Parser.TryParse(inner[..commaIndex], out double x, out _) &&
                    System.Buffers.Text.Utf8Parser.TryParse(inner[(commaIndex + 1)..], out double y, out _))
                {
                    return new Point(x, y);
                }
            }
        }
        throw new FormatException($"Invalid point format: {Utf8.GetString(buffer)}");
    }

    public static Interval DecodeIntervalText(ReadOnlySpan<byte> buffer)
    {
        // Format examples: "1 year 2 mons 3 days 04:05:06" or "00:00:00" or "1 year" etc.
        var interval = new Interval();
        
        int i = 0;
        while (i < buffer.Length)
        {
            // Skip whitespace
            while (i < buffer.Length && buffer[i] == ' ') i++;
            if (i >= buffer.Length) break;
            
            // Check for time component (HH:MM:SS or -HH:MM:SS)
            bool isNegativeTime = buffer[i] == '-';
            if (isNegativeTime) i++;
            
            if (i < buffer.Length && buffer[i] >= '0' && buffer[i] <= '9')
            {
                int numStart = i;
                while (i < buffer.Length && ((buffer[i] >= '0' && buffer[i] <= '9') || buffer[i] == '.')) i++;
                
                // Check if this is a time component (contains ':')
                if (i < buffer.Length && buffer[i] == ':')
                {
                    // Parse time component HH:MM:SS.microseconds
                    System.Buffers.Text.Utf8Parser.TryParse(buffer[numStart..i], out int hours, out _);
                    i++; // skip ':'
                    int minStart = i;
                    while (i < buffer.Length && buffer[i] >= '0' && buffer[i] <= '9') i++;
                    System.Buffers.Text.Utf8Parser.TryParse(buffer[minStart..i], out int minutes, out _);
                    
                    int seconds = 0;
                    int microseconds = 0;
                    if (i < buffer.Length && buffer[i] == ':')
                    {
                        i++; // skip ':'
                        int secStart = i;
                        while (i < buffer.Length && buffer[i] >= '0' && buffer[i] <= '9') i++;
                        System.Buffers.Text.Utf8Parser.TryParse(buffer[secStart..i], out seconds, out _);
                        
                        if (i < buffer.Length && buffer[i] == '.')
                        {
                            i++; // skip '.'
                            int microStart = i;
                            while (i < buffer.Length && buffer[i] >= '0' && buffer[i] <= '9') i++;
                            System.Buffers.Text.Utf8Parser.TryParse(buffer[microStart..i], out microseconds, out _);
                            // Pad or truncate to 6 digits
                            int digits = i - microStart;
                            while (digits < 6) { microseconds *= 10; digits++; }
                            while (digits > 6) { microseconds /= 10; digits--; }
                        }
                    }
                    
                    if (isNegativeTime)
                    {
                        hours = -hours;
                        minutes = -minutes;
                        seconds = -seconds;
                        microseconds = -microseconds;
                    }
                    
                    interval.Hours = hours;
                    interval.Minutes = minutes;
                    interval.Seconds = seconds;
                    interval.Microseconds = microseconds;
                }
                else
                {
                    // Parse number followed by unit
                    System.Buffers.Text.Utf8Parser.TryParse(buffer[numStart..i], out int value, out _);
                    if (isNegativeTime) value = -value;
                    
                    // Skip whitespace
                    while (i < buffer.Length && buffer[i] == ' ') i++;
                    
                    // Read unit
                    int unitStart = i;
                    while (i < buffer.Length && ((buffer[i] >= 'a' && buffer[i] <= 'z') || (buffer[i] >= 'A' && buffer[i] <= 'Z'))) i++;
                    var unit = buffer[unitStart..i];
                    
                    if (StartsWithIgnoreCase(unit, "year"u8))
                        interval.Years = value;
                    else if (StartsWithIgnoreCase(unit, "mon"u8))
                        interval.Months = value;
                    else if (StartsWithIgnoreCase(unit, "day"u8))
                        interval.Days = value;
                    else if (StartsWithIgnoreCase(unit, "hour"u8))
                        interval.Hours = value;
                    else if (StartsWithIgnoreCase(unit, "min"u8))
                        interval.Minutes = value;
                    else if (StartsWithIgnoreCase(unit, "sec"u8))
                        interval.Seconds = value;
                }
            }
            else
            {
                i++;
            }
        }
        
        return interval;
    }

    private static bool StartsWithIgnoreCase(ReadOnlySpan<byte> span, ReadOnlySpan<byte> prefix)
    {
        if (span.Length < prefix.Length)
        {
            return false;
        }
        for (int i = 0; i < prefix.Length; i++)
        {
            byte a = span[i];
            byte b = prefix[i];
            // Convert to lowercase for comparison
            if (a >= 'A' && a <= 'Z')
            {
                a = (byte)(a + 32);
            }
            if (b >= 'A' && b <= 'Z')
            {
                b = (byte)(b + 32);
            }
            if (a != b)
            {
                return false;
            }
        }
        return true;
    }

    // Public text array decode methods - aliases for consistent naming with PgValue

    public static int[] DecodeInt32ArrayText(ReadOnlySpan<byte> buffer) => DecodeInt4ArrayText(buffer);
    public static short[] DecodeInt16ArrayText(ReadOnlySpan<byte> buffer) => DecodeInt2ArrayText(buffer);
    public static long[] DecodeInt64ArrayText(ReadOnlySpan<byte> buffer) => DecodeInt8ArrayText(buffer);
    public static float[] DecodeFloatArrayText(ReadOnlySpan<byte> buffer) => DecodeFloat4ArrayText(buffer);
    public static double[] DecodeDoubleArrayText(ReadOnlySpan<byte> buffer) => DecodeFloat8ArrayText(buffer);
    public static string[] DecodeStringArrayText(ReadOnlySpan<byte> buffer) => DecodeTextArrayText(buffer);
    public static DateTime[] DecodeDateTimeArrayText(ReadOnlySpan<byte> buffer) => DecodeTimestampArrayText(buffer);
    public static DateTimeOffset[] DecodeDateTimeOffsetArrayText(ReadOnlySpan<byte> buffer) => DecodeTimestamptzArrayText(buffer);
    public static Guid[] DecodeGuidArrayText(ReadOnlySpan<byte> buffer) => DecodeUuidArrayText(buffer);

    public static int[] DecodeInt4ArrayText(ReadOnlySpan<byte> buffer)
    {
        // Format: {1,2,3,4,5}
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        int count = 1;
        foreach (var b in inner)
            if (b == ',') count++;

        var result = new int[count];
        int index = 0;
        foreach (var range in inner.Split((byte)','))
        {
            var element = inner[range];
            // NULL handling - treat as 0
            if (element.Length == 4 && 
                element[0] is (byte)'N' or (byte)'n' &&
                element[1] is (byte)'U' or (byte)'u' &&
                element[2] is (byte)'L' or (byte)'l' &&
                element[3] is (byte)'L' or (byte)'l')
            {
                result[index++] = 0;
            }
            else if (Utf8Parser.TryParse(element, out int value, out _))
            {
                result[index++] = value;
            }
        }
        return result[..index];
    }

    public static string[] DecodeTextArrayText(ReadOnlySpan<byte> buffer)
    {
        // Format: {a,b,c} or {"a","b","c"} for quoted strings
        if (buffer.Length < 2 || buffer[0] != '{' || buffer[^1] != '}')
            return Array.Empty<string>();
        
        if (buffer.Length == 2) // Empty array "{}"
            return Array.Empty<string>();
        
        var result = new List<string>();
        int i = 1; // Skip opening brace
        int end = buffer.Length - 1; // Before closing brace
        
        while (i < end)
        {
            if (buffer[i] == '"')
            {
                // Quoted string
                i++; // Skip opening quote
                var sb = new System.Text.StringBuilder();
                while (i < end)
                {
                    if (buffer[i] == '\\' && i + 1 < end)
                    {
                        sb.Append((char)buffer[i + 1]);
                        i += 2;
                    }
                    else if (buffer[i] == '"')
                    {
                        break;
                    }
                    else
                    {
                        sb.Append((char)buffer[i]);
                        i++;
                    }
                }
                result.Add(sb.ToString());
                i++; // Skip closing quote
                if (i < end && buffer[i] == ',')
                    i++; // Skip comma
            }
            else if (buffer[i] == ',')
            {
                i++;
            }
            else
            {
                // Unquoted string
                int start = i;
                while (i < end && buffer[i] != ',')
                    i++;
                var element = buffer[start..i];
                if (element.Length == 4 && 
                    (element[0] == 'N' || element[0] == 'n') &&
                    (element[1] == 'U' || element[1] == 'u') &&
                    (element[2] == 'L' || element[2] == 'l') &&
                    (element[3] == 'L' || element[3] == 'l'))
                {
                    result.Add(null!);
                }
                else
                {
                    result.Add(Utf8.GetString(element));
                }
                if (i < end && buffer[i] == ',')
                    i++;
            }
        }
        
        return result.ToArray();
    }

    public static Line DecodeLineText(ReadOnlySpan<byte> buffer)
    {
        // Format: {A,B,C}
        if (buffer.Length >= 5 && buffer[0] == '{' && buffer[^1] == '}')
        {
            var inner = buffer[1..^1];
            Span<Range> ranges = stackalloc Range[3];
            int count = SplitByteSpan(inner, (byte)',', ranges);
            if (count == 3)
            {
                System.Buffers.Text.Utf8Parser.TryParse(inner[ranges[0]], out double a, out _);
                System.Buffers.Text.Utf8Parser.TryParse(inner[ranges[1]], out double b, out _);
                System.Buffers.Text.Utf8Parser.TryParse(inner[ranges[2]], out double c, out _);
                return new Line(a, b, c);
            }
        }
        throw new FormatException($"Invalid line format: {Utf8.GetString(buffer)}");
    }

    public static LineSegment DecodeLsegText(ReadOnlySpan<byte> buffer)
    {
        // Format: [(x1,y1),(x2,y2)]
        var text = Utf8.GetString(buffer);
        var points = ParsePointList(text);
        if (points.Count == 2)
        {
            return new LineSegment(points[0], points[1]);
        }
        throw new FormatException($"Invalid lseg format: {text}");
    }

    public static Box DecodeBoxText(ReadOnlySpan<byte> buffer)
    {
        // Format: (x1,y1),(x2,y2)
        var text = Utf8.GetString(buffer);
        var points = ParsePointList(text);
        if (points.Count == 2)
        {
            return new Box(points[0], points[1]);
        }
        throw new FormatException($"Invalid box format: {text}");
    }

    public static Data.Path DecodePathText(ReadOnlySpan<byte> buffer)
    {
        // Format: [(x1,y1),(x2,y2),...] for open, ((x1,y1),(x2,y2),...) for closed
        ReadOnlySpan<char> text = Utf8.GetString(buffer).AsSpan();
        bool isOpen = text.Length > 0 && text[0] == '[';
        var points = ParsePointList(text);
        return new Data.Path(isOpen, points);
    }

    public static Polygon DecodePolygonText(ReadOnlySpan<byte> buffer)
    {
        // Format: ((x1,y1),(x2,y2),...)
        ReadOnlySpan<char> text = Utf8.GetString(buffer).AsSpan();
        var points = ParsePointList(text);
        return new Polygon(points);
    }

    public static Circle DecodeCircleText(ReadOnlySpan<byte> buffer)
    {
        // Format: <(x,y),r>
        ReadOnlySpan<char> text = Utf8.GetString(buffer).AsSpan();
        if (text.Length >= 2 && text[0] == '<' && text[^1] == '>')
        {
            var inner = text[1..^1];
            // Find the last comma which separates center from radius
            int lastComma = inner.LastIndexOf(',');
            if (lastComma > 0)
            {
                var centerPart = inner[..lastComma];
                var radiusPart = inner[(lastComma + 1)..];
                
                // Parse center point
                var points = ParsePointList(centerPart);
                if (points.Count == 1 && double.TryParse(radiusPart, out double radius))
                {
                    return new Circle(points[0], radius);
                }
            }
        }
        throw new FormatException($"Invalid circle format: {text.ToString()}");
    }

    public static Inet DecodeInetText(ReadOnlySpan<byte> buffer)
    {
        // Format: 192.168.1.1 or 192.168.1.1/24 or ::1 etc.
        ReadOnlySpan<char> text = Utf8.GetString(buffer).AsSpan();
        int slashIndex = text.IndexOf('/');
        if (slashIndex >= 0)
        {
            var address = IPAddress.Parse(text[..slashIndex]);
            var netmask = int.Parse(text[(slashIndex + 1)..]);
            return new Inet().SetAddress(address).SetNetmask(netmask);
        }
        else
        {
            var address = IPAddress.Parse(text);
            int defaultNetmask = address.AddressFamily == AddressFamily.InterNetwork ? 32 : 128;
            return new Inet().SetAddress(address).SetNetmask(defaultNetmask);
        }
    }

    public static Cidr DecodeCidrText(ReadOnlySpan<byte> buffer)
    {
        // Format: 192.168.1.0/24
        ReadOnlySpan<char> text = Utf8.GetString(buffer).AsSpan();
        int slashIndex = text.IndexOf('/');
        if (slashIndex >= 0)
        {
            var address = IPAddress.Parse(text[..slashIndex]);
            var netmask = int.Parse(text[(slashIndex + 1)..]);
            return new Cidr().SetAddress(address).SetNetmask(netmask);
        }
        else
        {
            var address = IPAddress.Parse(text);
            int defaultNetmask = address.AddressFamily == AddressFamily.InterNetwork ? 32 : 128;
            return new Cidr().SetAddress(address).SetNetmask(defaultNetmask);
        }
    }

    public static bool[] DecodeBoolArrayText(ReadOnlySpan<byte> buffer)
    {
        // Format: {t,f,t}
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        // Count elements to pre-allocate
        int count = 1;
        foreach (var b in inner)
            if (b == ',') count++;

        var result = new bool[count];
        int index = 0;
        foreach (var range in inner.Split((byte)','))
        {
            var element = inner[range];
            if (element.Length > 0)
            {
                result[index++] = element[0] is (byte)'t' or (byte)'T' or (byte)'1';
            }
        }
        return result;
    }

    public static double[] DecodeFloat8ArrayText(ReadOnlySpan<byte> buffer)
    {
        // Format: {1.1,2.2,3.3}
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        int count = 1;
        foreach (var b in inner)
            if (b == ',') count++;

        var result = new double[count];
        int index = 0;
        foreach (var range in inner.Split((byte)','))
        {
            if (Utf8Parser.TryParse(inner[range], out double value, out _))
                result[index++] = value;
        }
        return result[..index];
    }

    public static short[] DecodeInt2ArrayText(ReadOnlySpan<byte> buffer)
    {
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        int count = 1;
        foreach (var b in inner)
            if (b == ',') count++;

        var result = new short[count];
        int index = 0;
        foreach (var range in inner.Split((byte)','))
        {
            if (Utf8Parser.TryParse(inner[range], out short value, out _))
                result[index++] = value;
        }
        return result[..index];
    }

    public static long[] DecodeInt8ArrayText(ReadOnlySpan<byte> buffer)
    {
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        int count = 1;
        foreach (var b in inner)
            if (b == ',') count++;

        var result = new long[count];
        int index = 0;
        foreach (var range in inner.Split((byte)','))
        {
            if (Utf8Parser.TryParse(inner[range], out long value, out _))
                result[index++] = value;
        }
        return result[..index];
    }

    public static float[] DecodeFloat4ArrayText(ReadOnlySpan<byte> buffer)
    {
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        int count = 1;
        foreach (var b in inner)
            if (b == ',') count++;

        var result = new float[count];
        int index = 0;
        foreach (var range in inner.Split((byte)','))
        {
            if (Utf8Parser.TryParse(inner[range], out float value, out _))
                result[index++] = value;
        }
        return result[..index];
    }

    public static DateOnly[] DecodeDateArrayText(ReadOnlySpan<byte> buffer)
    {
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        int count = 1;
        foreach (var b in inner)
            if (b == ',') count++;

        var result = new DateOnly[count];
        int index = 0;
        foreach (var range in inner.Split((byte)','))
        {
            var element = inner[range];
            if (element.Length > 0 && DateOnly.TryParse(Utf8.GetString(element), out var date))
                result[index++] = date;
        }
        return result[..index];
    }

    public static DateTime[] DecodeTimestampArrayText(ReadOnlySpan<byte> buffer)
    {
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        // Timestamps may be quoted: {"2023-01-15 10:30:00","2023-06-20 15:45:00"}
        var result = new List<DateTime>();
        foreach (var range in inner.Split((byte)','))
        {
            var element = inner[range];
            // Strip quotes if present
            if (element.Length >= 2 && element[0] == '"' && element[^1] == '"')
                element = element[1..^1];
            if (element.Length > 0 && DateTime.TryParse(Utf8.GetString(element), out var dt))
                result.Add(dt);
        }
        return [.. result];
    }

    public static DateTimeOffset[] DecodeTimestamptzArrayText(ReadOnlySpan<byte> buffer)
    {
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        var result = new List<DateTimeOffset>();
        foreach (var range in inner.Split((byte)','))
        {
            var element = inner[range];
            // Strip quotes if present
            if (element.Length >= 2 && element[0] == '"' && element[^1] == '"')
                element = element[1..^1];
            if (element.Length > 0 && DateTimeOffset.TryParse(Utf8.GetString(element), out var dto))
                result.Add(dto);
        }
        return [.. result];
    }

    public static Guid[] DecodeUuidArrayText(ReadOnlySpan<byte> buffer)
    {
        if (!TryGetArrayInner(buffer, out var inner))
            return [];

        int count = 1;
        foreach (var b in inner)
            if (b == ',') count++;

        var result = new Guid[count];
        int index = 0;
        foreach (var range in inner.Split((byte)','))
        {
            if (Utf8Parser.TryParse(inner[range], out Guid value, out _))
                result[index++] = value;
        }
        return result[..index];
    }

    /// <summary>
    /// Extracts the inner content of a PostgreSQL array, stripping the outer braces.
    /// </summary>
    private static bool TryGetArrayInner(ReadOnlySpan<byte> buffer, out ReadOnlySpan<byte> inner)
    {
        if (buffer.Length < 2 || buffer[0] != '{' || buffer[^1] != '}')
        {
            inner = default;
            return false;
        }
        if (buffer.Length == 2) // Empty array "{}"
        {
            inner = default;
            return false;
        }
        inner = buffer[1..^1];
        return true;
    }

    private static List<Point> ParsePointList(ReadOnlySpan<char> text)
    {
        // Parse a list of points from formats like ((x1,y1),(x2,y2)) or [(x1,y1),(x2,y2)]
        var points = new List<Point>();
        int i = 0;
        int depth = 0;
        
        while (i < text.Length)
        {
            if (text[i] == '(')
            {
                depth++;
                if (depth == 2) // We're inside an inner point
                {
                    int start = i + 1;
                    int end = text[i..].IndexOf(')');
                    if (end > 0)
                    {
                        end += i; // Adjust to absolute position
                        var pointStr = text[start..end];
                        int comma = pointStr.IndexOf(',');
                        if (comma > 0)
                        {
                            if (double.TryParse(pointStr[..comma], out double x) &&
                                double.TryParse(pointStr[(comma + 1)..], out double y))
                            {
                                points.Add(new Point(x, y));
                            }
                        }
                        i = end;
                        depth--;
                    }
                }
            }
            else if (text[i] == '[')
            {
                // Treat [ as depth=1 for open paths like [(x1,y1),(x2,y2)]
                depth = 1;
            }
            else if (text[i] == ')' || text[i] == ']')
            {
                depth = Math.Max(0, depth - 1);
            }
            i++;
        }
        
        // If we didn't find nested points, try parsing as simple point list (for formats like (x,y),(x,y))
        if (points.Count == 0)
        {
            i = 0;
            while (i < text.Length)
            {
                int start = text[i..].IndexOf('(');
                if (start < 0) break;
                start += i; // Adjust to absolute position
                
                int end = text[start..].IndexOf(')');
                if (end < 0) break;
                end += start; // Adjust to absolute position
                
                var pointStr = text[(start + 1)..end];
                int comma = pointStr.IndexOf(',');
                if (comma > 0)
                {
                    if (double.TryParse(pointStr[..comma], out double x) &&
                        double.TryParse(pointStr[(comma + 1)..], out double y))
                    {
                        points.Add(new Point(x, y));
                    }
                }
                
                i = end + 1;
            }
        }
        
        return points;
    }

    private static int SplitByteSpan(ReadOnlySpan<byte> span, byte separator, Span<Range> ranges)
    {
        int count = 0;
        int start = 0;
        
        for (int i = 0; i <= span.Length && count < ranges.Length; i++)
        {
            if (i == span.Length || span[i] == separator)
            {
                ranges[count++] = new Range(start, i);
                start = i + 1;
            }
        }
        
        return count;
    }

    #endregion
}
