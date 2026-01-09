// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Runtime.CompilerServices;
using Vertx.PgClient.Codec;
using Vertx.PgClient.Data;

namespace Vertx.PgClient;

/// <summary>
/// Represents a PostgreSQL value with its associated data type.
/// This is a discriminated union that holds typed values without boxing for common types.
/// </summary>
public readonly struct PgValue
{
    /// <summary>
    /// Represents a null PgValue.
    /// </summary>
    public static readonly PgValue Null = default;

    // Storage for small value types (up to 8 bytes: bool, short, int, long, float, double)
    // We use Unsafe.As to reinterpret the bits for different types
    private readonly long _primitiveValue;
    
    // Second primitive storage for 16-byte value types (Guid, DateTimeOffset)
    // For Guid: stores the second 8 bytes
    // For DateTimeOffset: stores the offset in minutes (as short)
    private readonly long _primitiveValue2;
    
    // Reference type storage for strings, arrays, complex types, and large value types (decimal)
    private readonly object? _objectValue;

    // Type discriminator and metadata
    private readonly DataType _dataType;
    private readonly PgValueKind _kind;

    /// <summary>
    /// Gets the data type of this value.
    /// </summary>
    public DataType DataType => _dataType;

    /// <summary>
    /// Gets whether this value is null.
    /// </summary>
    public bool IsNull => _kind == PgValueKind.Null;

    /// <summary>
    /// Gets the kind of value stored.
    /// </summary>
    public PgValueKind Kind => _kind;

    #region Constructors

    private PgValue(DataType dataType, PgValueKind kind)
    {
        _dataType = dataType;
        _kind = kind;
    }

    /// <summary>
    /// Creates a null PgValue with the specified data type.
    /// </summary>
    public static PgValue CreateNull(DataType dataType) => new PgValue(dataType, PgValueKind.Null);

    /// <summary>
    /// Creates a PgValue from any object, determining the appropriate type at runtime.
    /// </summary>
    public static PgValue From<T>(T? value)
    {
        return value switch
        {
            null => Null,
            PgValue pv => pv,
            bool b => new PgValue(b),
            short s => new PgValue(s),
            int i => new PgValue(i),
            long l => new PgValue(l),
            float f => new PgValue(f),
            double d => new PgValue(d),
            decimal m => new PgValue(m),
            string str => new PgValue(str),
            DateOnly date => new PgValue(date),
            TimeOnly time => new PgValue(time),
            DateTime dt => new PgValue(dt),
            DateTimeOffset dto => new PgValue(dto),
            Guid g => new PgValue(g),
            byte[] bytes => new PgValue(bytes),
            int[] intArr => new PgValue(intArr, DataType.Int4Array),
            long[] longArr => new PgValue(longArr, DataType.Int8Array),
            short[] shortArr => new PgValue(shortArr, DataType.Int2Array),
            float[] floatArr => new PgValue(floatArr, DataType.Float4Array),
            double[] doubleArr => new PgValue(doubleArr, DataType.Float8Array),
            bool[] boolArr => new PgValue(boolArr, DataType.BoolArray),
            string[] strArr => new PgValue(strArr, DataType.TextArray),
            Guid[] guidArr => new PgValue(guidArr, DataType.UuidArray),
            DateTime[] dtArr => new PgValue(dtArr, DataType.TimestampArray),
            DateTimeOffset[] dtoArr => new PgValue(dtoArr, DataType.TimestamptzArray),
            DateOnly[] dateArr => new PgValue(dateArr, DataType.DateArray),
            _ => new PgValue(value, DataType.Unknown)
        };
    }

    public PgValue(bool value, DataType? dataType = null)
    {
        _primitiveValue = value ? 1L : 0L;
        _dataType = dataType ?? DataType.Bool;
        _kind = PgValueKind.Bool;
    }

    public PgValue(short value, DataType? dataType = null)
    {
        _primitiveValue = value;
        _dataType = dataType ?? DataType.Int2;
        _kind = PgValueKind.Int16;
    }

    public PgValue(int value, DataType? dataType = null)
    {
        _primitiveValue = value;
        _dataType = dataType ?? DataType.Int4;
        _kind = PgValueKind.Int32;
    }

    public PgValue(long value, DataType? dataType = null)
    {
        _primitiveValue = value;
        _dataType = dataType ?? DataType.Int8;
        _kind = PgValueKind.Int64;
    }

    public PgValue(float value, DataType? dataType = null)
    {
        _primitiveValue = Unsafe.As<float, int>(ref value);
        _dataType = dataType ?? DataType.Float4;
        _kind = PgValueKind.Float;
    }

    public PgValue(double value, DataType? dataType = null)
    {
        _primitiveValue = Unsafe.As<double, long>(ref value);
        _dataType = dataType ?? DataType.Float8;
        _kind = PgValueKind.Double;
    }

    public PgValue(decimal value, DataType? dataType = null)
    {
        _objectValue = value; // Decimal is 16 bytes, store as object (boxed)
        _dataType = dataType ?? DataType.Numeric;
        _kind = PgValueKind.Decimal;
    }

    public PgValue(string? value, DataType? dataType = null)
    {
        if (value is null)
        {
            _kind = PgValueKind.Null;
            _dataType = dataType ?? DataType.Text;
        }
        else
        {
            _objectValue = value;
            _dataType = dataType ?? DataType.Text;
            _kind = PgValueKind.String;
        }
    }

    public PgValue(DateOnly value, DataType? dataType = null)
    {
        _primitiveValue = value.DayNumber;
        _dataType = dataType ?? DataType.Date;
        _kind = PgValueKind.DateOnly;
    }

    public PgValue(TimeOnly value, DataType? dataType = null)
    {
        _primitiveValue = value.Ticks;
        _dataType = dataType ?? DataType.Time;
        _kind = PgValueKind.TimeOnly;
    }

    public PgValue(DateTime value, DataType? dataType = null)
    {
        _primitiveValue = value.Ticks;
        _dataType = dataType ?? DataType.Timestamp;
        _kind = PgValueKind.DateTime;
    }

    public PgValue(DateTimeOffset value, DataType? dataType = null)
    {
        // Store original DateTime ticks and offset minutes separately
        _primitiveValue = value.Ticks;
        _primitiveValue2 = (long)value.Offset.TotalMinutes;
        _dataType = dataType ?? DataType.Timestamptz;
        _kind = PgValueKind.DateTimeOffset;
    }

    public PgValue(Guid value, DataType? dataType = null)
    {
        // Store Guid as two longs using Unsafe to reinterpret the 16 bytes
        Span<byte> bytes = stackalloc byte[16];
        value.TryWriteBytes(bytes);
        _primitiveValue = Unsafe.ReadUnaligned<long>(ref bytes[0]);
        _primitiveValue2 = Unsafe.ReadUnaligned<long>(ref bytes[8]);
        _dataType = dataType ?? DataType.Uuid;
        _kind = PgValueKind.Guid;
    }

    public PgValue(byte[]? value, DataType? dataType = null)
    {
        if (value is null)
        {
            _kind = PgValueKind.Null;
            _dataType = dataType ?? DataType.Bytea;
        }
        else
        {
            _objectValue = value;
            _dataType = dataType ?? DataType.Bytea;
            _kind = PgValueKind.ByteArray;
        }
    }

    /// <summary>
    /// Creates a PgValue from a reference type (for complex types like Point, Interval, arrays, etc.)
    /// </summary>
    public PgValue(object? value, DataType dataType)
    {
        if (value is null)
        {
            _kind = PgValueKind.Null;
            _dataType = dataType;
        }
        else
        {
            _objectValue = value;
            _dataType = dataType;
            _kind = PgValueKind.Object;
        }
    }

    #endregion

    #region Getters - No Boxing

    /// <summary>
    /// Gets the value as a boolean.
    /// </summary>
    public bool GetBoolean()
    {
        return _kind switch
        {
            PgValueKind.Bool => _primitiveValue != 0,
            PgValueKind.Int16 => _primitiveValue != 0,
            PgValueKind.Int32 => _primitiveValue != 0,
            PgValueKind.Int64 => _primitiveValue != 0,
            PgValueKind.String => bool.Parse((string)_objectValue!),
            PgValueKind.Null => false,
            _ => Convert.ToBoolean(GetObject())
        };
    }

    /// <summary>
    /// Gets the value as a short.
    /// </summary>
    public short GetInt16()
    {
        return _kind switch
        {
            PgValueKind.Int16 => (short)_primitiveValue,
            PgValueKind.Int32 => (short)_primitiveValue,
            PgValueKind.Int64 => (short)_primitiveValue,
            PgValueKind.Float => (short)GetFloatRaw(),
            PgValueKind.Double => (short)GetDoubleRaw(),
            PgValueKind.Bool => _primitiveValue != 0 ? (short)1 : (short)0,
            PgValueKind.Decimal => (short)(decimal)_objectValue!,
            PgValueKind.String => short.Parse((string)_objectValue!),
            PgValueKind.Null => 0,
            _ => Convert.ToInt16(GetObject())
        };
    }

    /// <summary>
    /// Gets the value as an integer.
    /// </summary>
    public int GetInt32()
    {
        return _kind switch
        {
            PgValueKind.Int32 => (int)_primitiveValue,
            PgValueKind.Int16 => (int)_primitiveValue,
            PgValueKind.Int64 => (int)_primitiveValue,
            PgValueKind.Float => (int)GetFloatRaw(),
            PgValueKind.Double => (int)GetDoubleRaw(),
            PgValueKind.Bool => _primitiveValue != 0 ? 1 : 0,
            PgValueKind.Decimal => (int)(decimal)_objectValue!,
            PgValueKind.String => int.Parse((string)_objectValue!),
            PgValueKind.Null => 0,
            _ => Convert.ToInt32(GetObject())
        };
    }

    /// <summary>
    /// Gets the value as a long.
    /// </summary>
    public long GetInt64()
    {
        return _kind switch
        {
            PgValueKind.Int64 => _primitiveValue,
            PgValueKind.Int32 => _primitiveValue,
            PgValueKind.Int16 => _primitiveValue,
            PgValueKind.Float => (long)GetFloatRaw(),
            PgValueKind.Double => (long)GetDoubleRaw(),
            PgValueKind.Bool => _primitiveValue != 0 ? 1L : 0L,
            PgValueKind.Decimal => (long)(decimal)_objectValue!,
            PgValueKind.String => long.Parse((string)_objectValue!),
            PgValueKind.Null => 0,
            _ => Convert.ToInt64(GetObject())
        };
    }

    /// <summary>
    /// Gets the value as a float.
    /// </summary>
    public float GetFloat()
    {
        return _kind switch
        {
            PgValueKind.Float => GetFloatRaw(),
            PgValueKind.Double => (float)GetDoubleRaw(),
            PgValueKind.Int16 => _primitiveValue,
            PgValueKind.Int32 => _primitiveValue,
            PgValueKind.Int64 => _primitiveValue,
            PgValueKind.Decimal => (float)(decimal)_objectValue!,
            PgValueKind.String => float.Parse((string)_objectValue!),
            PgValueKind.Null => 0,
            _ => Convert.ToSingle(GetObject())
        };
    }

    /// <summary>
    /// Gets the value as a double.
    /// </summary>
    public double GetDouble()
    {
        return _kind switch
        {
            PgValueKind.Double => GetDoubleRaw(),
            PgValueKind.Float => GetFloatRaw(),
            PgValueKind.Int16 => _primitiveValue,
            PgValueKind.Int32 => _primitiveValue,
            PgValueKind.Int64 => _primitiveValue,
            PgValueKind.Decimal => (double)(decimal)_objectValue!,
            PgValueKind.String => double.Parse((string)_objectValue!),
            PgValueKind.Null => 0,
            _ => Convert.ToDouble(GetObject())
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private float GetFloatRaw()
    {
        var intVal = (int)_primitiveValue;
        return Unsafe.As<int, float>(ref intVal);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private double GetDoubleRaw()
    {
        var longVal = _primitiveValue;
        return Unsafe.As<long, double>(ref longVal);
    }

    /// <summary>
    /// Gets the value as a decimal.
    /// </summary>
    public decimal GetDecimal()
    {
        return _kind switch
        {
            PgValueKind.Decimal => (decimal)_objectValue!,
            PgValueKind.Double => (decimal)GetDoubleRaw(),
            PgValueKind.Float => (decimal)GetFloatRaw(),
            PgValueKind.Int16 => _primitiveValue,
            PgValueKind.Int32 => _primitiveValue,
            PgValueKind.Int64 => _primitiveValue,
            PgValueKind.String => decimal.Parse((string)_objectValue!),
            PgValueKind.Null => 0,
            PgValueKind.Object when _objectValue is Money m => m.BigDecimalValue,
            _ => Convert.ToDecimal(GetObject())
        };
    }

    /// <summary>
    /// Gets the value as a string.
    /// </summary>
    public string? GetString()
    {
        return _kind switch
        {
            PgValueKind.String => (string)_objectValue!,
            PgValueKind.Null => null,
            PgValueKind.Bool => _primitiveValue != 0 ? "true" : "false",
            PgValueKind.Int16 => _primitiveValue.ToString(),
            PgValueKind.Int32 => _primitiveValue.ToString(),
            PgValueKind.Int64 => _primitiveValue.ToString(),
            PgValueKind.Float => GetFloatRaw().ToString(),
            PgValueKind.Double => GetDoubleRaw().ToString(),
            PgValueKind.Decimal => ((decimal)_objectValue!).ToString(),
            PgValueKind.DateOnly => DateOnly.FromDayNumber((int)_primitiveValue).ToString(),
            PgValueKind.TimeOnly => new TimeOnly(_primitiveValue).ToString(),
            PgValueKind.DateTime => new DateTime(_primitiveValue).ToString(),
            PgValueKind.DateTimeOffset => GetDateTimeOffset().ToString(),
            PgValueKind.Guid => ReconstructGuid().ToString(),
            _ => _objectValue?.ToString()
        };
    }

    /// <summary>
    /// Gets the value as a DateTime.
    /// </summary>
    public DateTime GetDateTime()
    {
        return _kind switch
        {
            PgValueKind.DateTime => new DateTime(_primitiveValue),
            PgValueKind.DateTimeOffset => GetDateTimeOffset().DateTime,
            PgValueKind.DateOnly => DateOnly.FromDayNumber((int)_primitiveValue).ToDateTime(TimeOnly.MinValue),
            PgValueKind.String => DateTime.Parse((string)_objectValue!),
            PgValueKind.Null => default,
            _ => (DateTime)GetObject()!
        };
    }

    /// <summary>
    /// Gets the value as a DateTimeOffset.
    /// </summary>
    public DateTimeOffset GetDateTimeOffset()
    {
        return _kind switch
        {
            PgValueKind.DateTimeOffset => new DateTimeOffset(_primitiveValue, TimeSpan.FromMinutes(_primitiveValue2)),
            PgValueKind.DateTime => new DateTimeOffset(new DateTime(_primitiveValue)),
            PgValueKind.DateOnly => new DateTimeOffset(DateOnly.FromDayNumber((int)_primitiveValue).ToDateTime(TimeOnly.MinValue)),
            PgValueKind.String => DateTimeOffset.Parse((string)_objectValue!),
            PgValueKind.Null => default,
            _ => (DateTimeOffset)GetObject()!
        };
    }

    /// <summary>
    /// Gets the value as a DateOnly.
    /// </summary>
    public DateOnly GetDateOnly()
    {
        return _kind switch
        {
            PgValueKind.DateOnly => DateOnly.FromDayNumber((int)_primitiveValue),
            PgValueKind.DateTime => DateOnly.FromDateTime(new DateTime(_primitiveValue)),
            PgValueKind.DateTimeOffset => DateOnly.FromDateTime(GetDateTimeOffset().DateTime),
            PgValueKind.String => DateOnly.Parse((string)_objectValue!),
            PgValueKind.Null => default,
            _ => (DateOnly)GetObject()!
        };
    }

    /// <summary>
    /// Gets the value as a TimeOnly.
    /// </summary>
    public TimeOnly GetTimeOnly()
    {
        return _kind switch
        {
            PgValueKind.TimeOnly => new TimeOnly(_primitiveValue),
            PgValueKind.DateTime => TimeOnly.FromDateTime(new DateTime(_primitiveValue)),
            PgValueKind.DateTimeOffset => TimeOnly.FromDateTime(GetDateTimeOffset().DateTime),
            PgValueKind.String => TimeOnly.Parse((string)_objectValue!),
            PgValueKind.Null => default,
            _ => (TimeOnly)GetObject()!
        };
    }

    /// <summary>
    /// Gets the value as a Guid.
    /// </summary>
    public Guid GetGuid()
    {
        return _kind switch
        {
            PgValueKind.Guid => ReconstructGuid(),
            PgValueKind.String => Guid.Parse((string)_objectValue!),
            PgValueKind.ByteArray => new Guid((byte[])_objectValue!),
            PgValueKind.Null => Guid.Empty,
            _ => (Guid)GetObject()!
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Guid ReconstructGuid()
    {
        Span<byte> bytes = stackalloc byte[16];
        Unsafe.WriteUnaligned(ref bytes[0], _primitiveValue);
        Unsafe.WriteUnaligned(ref bytes[8], _primitiveValue2);
        return new Guid(bytes);
    }

    /// <summary>
    /// Gets the value as a byte array.
    /// </summary>
    public byte[]? GetBytes()
    {
        return _kind switch
        {
            PgValueKind.ByteArray => (byte[])_objectValue!,
            PgValueKind.Null => null,
            PgValueKind.String => System.Text.Encoding.UTF8.GetBytes((string)_objectValue!),
            _ => _objectValue as byte[]
        };
    }

    /// <summary>
    /// Gets the value as a typed value. May involve boxing for value types.
    /// </summary>
    public T? Get<T>()
    {
        if (_kind == PgValueKind.Null)
        {
            return default;
        }

        // Fast paths for common types (boxing is unavoidable here due to generic return)
        if (typeof(T) == typeof(int)) return (T)(object)GetInt32();
        if (typeof(T) == typeof(long)) return (T)(object)GetInt64();
        if (typeof(T) == typeof(short)) return (T)(object)GetInt16();
        if (typeof(T) == typeof(bool)) return (T)(object)GetBoolean();
        if (typeof(T) == typeof(float)) return (T)(object)GetFloat();
        if (typeof(T) == typeof(double)) return (T)(object)GetDouble();
        if (typeof(T) == typeof(decimal)) return (T)(object)GetDecimal();
        if (typeof(T) == typeof(DateTime)) return (T)(object)GetDateTime();
        if (typeof(T) == typeof(DateOnly)) return (T)(object)GetDateOnly();
        if (typeof(T) == typeof(TimeOnly)) return (T)(object)GetTimeOnly();
        if (typeof(T) == typeof(DateTimeOffset)) return (T)(object)GetDateTimeOffset();
        if (typeof(T) == typeof(Guid)) return (T)(object)GetGuid();
        if (typeof(T) == typeof(string)) return (T?)(object?)GetString();

        // For reference types and other cases
        var obj = GetObject();
        if (obj is T typedValue)
        {
            return typedValue;
        }

        if (obj is null)
        {
            return default;
        }

        return (T)Convert.ChangeType(obj, typeof(T));
    }

    /// <summary>
    /// Gets the value as an object. This may involve boxing for value types.
    /// </summary>
    public object? GetObject()
    {
        return _kind switch
        {
            PgValueKind.Null => null,
            PgValueKind.Bool => _primitiveValue != 0,
            PgValueKind.Int16 => (short)_primitiveValue,
            PgValueKind.Int32 => (int)_primitiveValue,
            PgValueKind.Int64 => _primitiveValue,
            PgValueKind.Float => GetFloatRaw(),
            PgValueKind.Double => GetDoubleRaw(),
            PgValueKind.DateOnly => DateOnly.FromDayNumber((int)_primitiveValue),
            PgValueKind.TimeOnly => new TimeOnly(_primitiveValue),
            PgValueKind.DateTime => new DateTime(_primitiveValue),
            PgValueKind.DateTimeOffset => GetDateTimeOffset(),
            PgValueKind.Guid => ReconstructGuid(),
            _ => _objectValue
        };
    }

    #endregion

    #region Array Getters

    public int[]? GetInt32Array() => _objectValue as int[];
    public long[]? GetInt64Array() => _objectValue as long[];
    public short[]? GetInt16Array() => _objectValue as short[];
    public float[]? GetFloatArray() => _objectValue as float[];
    public double[]? GetDoubleArray() => _objectValue as double[];
    public bool[]? GetBooleanArray() => _objectValue as bool[];
    public string?[]? GetStringArray() => _objectValue as string?[];
    public Guid[]? GetGuidArray() => _objectValue as Guid[];
    public DateTime[]? GetDateTimeArray() => _objectValue as DateTime[];
    public DateTimeOffset[]? GetDateTimeOffsetArray() => _objectValue as DateTimeOffset[];
    public DateOnly[]? GetDateOnlyArray() => _objectValue as DateOnly[];

    // Tuple-compatible aliases for array getters
    public int[]? GetIntegerArray() => GetInt32Array();
    public long[]? GetLongArray() => GetInt64Array();
    public short[]? GetShortArray() => GetInt16Array();
    public DateOnly[]? GetDateArray() => GetDateOnlyArray();

    #endregion

    #region Tuple-compatible Aliases

    /// <summary>
    /// Alias for GetInt16() to match Tuple interface.
    /// </summary>
    public short GetShort() => GetInt16();

    /// <summary>
    /// Alias for GetInt32() to match Tuple interface.
    /// </summary>
    public int GetInteger() => GetInt32();

    /// <summary>
    /// Alias for GetInt64() to match Tuple interface.
    /// </summary>
    public long GetLong() => GetInt64();

    /// <summary>
    /// Alias for GetDateOnly() to match Tuple interface.
    /// </summary>
    public DateOnly GetDate() => GetDateOnly();

    /// <summary>
    /// Alias for GetTimeOnly() to match Tuple interface.
    /// </summary>
    public TimeOnly GetTime() => GetTimeOnly();

    #endregion

    #region Decode - Static Factory Methods

    /// <summary>
    /// Decodes a binary PostgreSQL value into a PgValue.
    /// </summary>
    public static PgValue DecodeBinary(DataType dataType, ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty)
        {
            return CreateNull(dataType);
        }

        return dataType.Id switch
        {
            DataTypeId.Bool => new PgValue(DataTypeCodec.DecodeBoolBinary(buffer)),
            DataTypeId.Int2 => new PgValue(DataTypeCodec.DecodeInt16Binary(buffer)),
            DataTypeId.Int4 => new PgValue(DataTypeCodec.DecodeInt32Binary(buffer)),
            DataTypeId.Int8 => new PgValue(DataTypeCodec.DecodeInt64Binary(buffer)),
            DataTypeId.Float4 => new PgValue(DataTypeCodec.DecodeFloatBinary(buffer)),
            DataTypeId.Float8 => new PgValue(DataTypeCodec.DecodeDoubleBinary(buffer)),
            DataTypeId.Char or DataTypeId.Varchar or DataTypeId.Bpchar or DataTypeId.Text or DataTypeId.Name =>
                new PgValue(DataTypeCodec.DecodeStringBinary(buffer), dataType),
            DataTypeId.Date => new PgValue(DataTypeCodec.DecodeDateBinary(buffer)),
            DataTypeId.Time => new PgValue(DataTypeCodec.DecodeTimeBinary(buffer)),
            DataTypeId.Timetz => new PgValue(DataTypeCodec.DecodeTimeTzBinary(buffer)),
            DataTypeId.Timestamp => new PgValue(DataTypeCodec.DecodeTimestampBinary(buffer)),
            DataTypeId.Timestamptz => new PgValue(DataTypeCodec.DecodeTimestampTzBinary(buffer)),
            DataTypeId.Bytea => new PgValue(DataTypeCodec.DecodeByteArrayBinary(buffer)),
            DataTypeId.Uuid => new PgValue(DataTypeCodec.DecodeGuidBinary(buffer)),
            DataTypeId.Json or DataTypeId.Jsonb => new PgValue(DataTypeCodec.DecodeJsonBinary(buffer), dataType),
            DataTypeId.Numeric => new PgValue(DataTypeCodec.DecodeNumericBinary(buffer)),
            // Complex types - stored as objects
            DataTypeId.Point => new PgValue(DataTypeCodec.DecodePointBinary(buffer), dataType),
            DataTypeId.Line => new PgValue(DataTypeCodec.DecodeLineBinary(buffer), dataType),
            DataTypeId.Lseg => new PgValue(DataTypeCodec.DecodeLineSegmentBinary(buffer), dataType),
            DataTypeId.Box => new PgValue(DataTypeCodec.DecodeBoxBinary(buffer), dataType),
            DataTypeId.Circle => new PgValue(DataTypeCodec.DecodeCircleBinary(buffer), dataType),
            DataTypeId.Path => new PgValue(DataTypeCodec.DecodePathBinary(buffer), dataType),
            DataTypeId.Polygon => new PgValue(DataTypeCodec.DecodePolygonBinary(buffer), dataType),
            DataTypeId.Interval => new PgValue(DataTypeCodec.DecodeIntervalBinary(buffer), dataType),
            DataTypeId.Inet => new PgValue(DataTypeCodec.DecodeInetBinary(buffer), dataType),
            DataTypeId.Cidr => new PgValue(DataTypeCodec.DecodeCidrBinary(buffer), dataType),
            DataTypeId.Money => new PgValue(DataTypeCodec.DecodeMoneyBinary(buffer), dataType),
            // Array types
            DataTypeId.BoolArray => new PgValue(DataTypeCodec.DecodeBoolArrayBinary(buffer), dataType),
            DataTypeId.Int2Array => new PgValue(DataTypeCodec.DecodeInt16ArrayBinary(buffer), dataType),
            DataTypeId.Int4Array => new PgValue(DataTypeCodec.DecodeInt32ArrayBinary(buffer), dataType),
            DataTypeId.Int8Array => new PgValue(DataTypeCodec.DecodeInt64ArrayBinary(buffer), dataType),
            DataTypeId.Float4Array => new PgValue(DataTypeCodec.DecodeFloatArrayBinary(buffer), dataType),
            DataTypeId.Float8Array => new PgValue(DataTypeCodec.DecodeDoubleArrayBinary(buffer), dataType),
            DataTypeId.NumericArray => new PgValue(DataTypeCodec.DecodeDecimalArrayBinary(buffer), dataType),
            DataTypeId.VarcharArray or DataTypeId.TextArray or DataTypeId.BpcharArray or DataTypeId.NameArray =>
                new PgValue(DataTypeCodec.DecodeStringArrayBinary(buffer), dataType),
            DataTypeId.DateArray => new PgValue(DataTypeCodec.DecodeDateArrayBinary(buffer), dataType),
            DataTypeId.TimestampArray => new PgValue(DataTypeCodec.DecodeDateTimeArrayBinary(buffer), dataType),
            DataTypeId.TimestamptzArray => new PgValue(DataTypeCodec.DecodeDateTimeOffsetArrayBinary(buffer), dataType),
            DataTypeId.UuidArray => new PgValue(DataTypeCodec.DecodeGuidArrayBinary(buffer), dataType),
            DataTypeId.ByteaArray => new PgValue(DataTypeCodec.DecodeByteArrayArrayBinary(buffer), dataType),
            _ => new PgValue(DataTypeCodec.DecodeStringBinary(buffer), dataType) // Unknown types decode as string
        };
    }

    /// <summary>
    /// Decodes a text PostgreSQL value into a PgValue.
    /// </summary>
    public static PgValue DecodeText(DataType dataType, ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty)
        {
            return CreateNull(dataType);
        }

        return dataType.Id switch
        {
            DataTypeId.Bool => new PgValue(buffer[0] == 't' || buffer[0] == '1'),
            DataTypeId.Int2 => new PgValue(DataTypeCodec.DecodeInt16Text(buffer)),
            DataTypeId.Int4 => new PgValue(DataTypeCodec.DecodeInt32Text(buffer)),
            DataTypeId.Int8 => new PgValue(DataTypeCodec.DecodeInt64Text(buffer)),
            DataTypeId.Float4 => new PgValue(DataTypeCodec.DecodeFloatText(buffer)),
            DataTypeId.Float8 => new PgValue(DataTypeCodec.DecodeDoubleText(buffer)),
            DataTypeId.Numeric => new PgValue(DataTypeCodec.DecodeDecimalText(buffer)),
            DataTypeId.Char or DataTypeId.Varchar or DataTypeId.Bpchar or DataTypeId.Text or DataTypeId.Name =>
                new PgValue(DataTypeCodec.DecodeStringBinary(buffer), dataType),
            DataTypeId.Date => new PgValue(DataTypeCodec.DecodeDateText(buffer)),
            DataTypeId.Time => new PgValue(DataTypeCodec.DecodeTimeText(buffer)),
            DataTypeId.Timestamp => new PgValue(DataTypeCodec.DecodeDateTimeText(buffer)),
            DataTypeId.Timestamptz => new PgValue(DataTypeCodec.DecodeDateTimeOffsetText(buffer)),
            DataTypeId.Uuid => new PgValue(DataTypeCodec.DecodeGuidText(buffer)),
            DataTypeId.Json or DataTypeId.Jsonb => new PgValue(DataTypeCodec.DecodeStringBinary(buffer), dataType),
            DataTypeId.Bytea => new PgValue(DataTypeCodec.DecodeByteaText(buffer)),
            // Complex types
            DataTypeId.Point => new PgValue(DataTypeCodec.DecodePointText(buffer), dataType),
            DataTypeId.Line => new PgValue(DataTypeCodec.DecodeLineText(buffer), dataType),
            DataTypeId.Lseg => new PgValue(DataTypeCodec.DecodeLsegText(buffer), dataType),
            DataTypeId.Box => new PgValue(DataTypeCodec.DecodeBoxText(buffer), dataType),
            DataTypeId.Path => new PgValue(DataTypeCodec.DecodePathText(buffer), dataType),
            DataTypeId.Polygon => new PgValue(DataTypeCodec.DecodePolygonText(buffer), dataType),
            DataTypeId.Circle => new PgValue(DataTypeCodec.DecodeCircleText(buffer), dataType),
            DataTypeId.Inet => new PgValue(DataTypeCodec.DecodeInetText(buffer), dataType),
            DataTypeId.Cidr => new PgValue(DataTypeCodec.DecodeCidrText(buffer), dataType),
            DataTypeId.Interval => new PgValue(DataTypeCodec.DecodeIntervalText(buffer), dataType),
            // Array types
            DataTypeId.BoolArray => new PgValue(DataTypeCodec.DecodeBoolArrayText(buffer), dataType),
            DataTypeId.Int2Array => new PgValue(DataTypeCodec.DecodeInt16ArrayText(buffer), dataType),
            DataTypeId.Int4Array => new PgValue(DataTypeCodec.DecodeInt32ArrayText(buffer), dataType),
            DataTypeId.Int8Array => new PgValue(DataTypeCodec.DecodeInt64ArrayText(buffer), dataType),
            DataTypeId.Float4Array => new PgValue(DataTypeCodec.DecodeFloatArrayText(buffer), dataType),
            DataTypeId.Float8Array => new PgValue(DataTypeCodec.DecodeDoubleArrayText(buffer), dataType),
            DataTypeId.VarcharArray or DataTypeId.TextArray or DataTypeId.BpcharArray or DataTypeId.NameArray =>
                new PgValue(DataTypeCodec.DecodeStringArrayText(buffer), dataType),
            DataTypeId.DateArray => new PgValue(DataTypeCodec.DecodeDateArrayText(buffer), dataType),
            DataTypeId.TimestampArray => new PgValue(DataTypeCodec.DecodeDateTimeArrayText(buffer), dataType),
            DataTypeId.TimestamptzArray => new PgValue(DataTypeCodec.DecodeDateTimeOffsetArrayText(buffer), dataType),
            DataTypeId.UuidArray => new PgValue(DataTypeCodec.DecodeGuidArrayText(buffer), dataType),
            _ => new PgValue(DataTypeCodec.DecodeStringBinary(buffer), dataType)
        };
    }

    #endregion

    #region Encode

    /// <summary>
    /// Encodes this value to binary PostgreSQL format.
    /// </summary>
    public int EncodeBinary(Span<byte> buffer) => EncodeBinary(buffer, null);

    /// <summary>
    /// Encodes this value to binary PostgreSQL format using the specified target data type.
    /// </summary>
    public int EncodeBinary(Span<byte> buffer, DataType? targetDataType)
    {
        if (_kind == PgValueKind.Null)
        {
            return 0;
        }

        return _kind switch
        {
            PgValueKind.Bool => DataTypeCodec.EncodeBoolBinary(_primitiveValue != 0, buffer),
            PgValueKind.Int16 => DataTypeCodec.EncodeInt16Binary((short)_primitiveValue, buffer),
            PgValueKind.Int32 => DataTypeCodec.EncodeInt32Binary((int)_primitiveValue, buffer),
            PgValueKind.Int64 => DataTypeCodec.EncodeInt64Binary(_primitiveValue, buffer),
            PgValueKind.Float => DataTypeCodec.EncodeFloatBinary(GetFloatRaw(), buffer),
            PgValueKind.Double => DataTypeCodec.EncodeDoubleBinary(GetDoubleRaw(), buffer),
            PgValueKind.Decimal => DataTypeCodec.EncodeNumericBinary((decimal)_objectValue!, buffer),
            PgValueKind.String => EncodeStringBinary(buffer, targetDataType),
            PgValueKind.DateOnly => DataTypeCodec.EncodeDateBinary(DateOnly.FromDayNumber((int)_primitiveValue), buffer),
            PgValueKind.TimeOnly => DataTypeCodec.EncodeTimeBinary(new TimeOnly(_primitiveValue), buffer),
            PgValueKind.DateTime => DataTypeCodec.EncodeTimestampBinary(new DateTime(_primitiveValue), buffer),
            PgValueKind.DateTimeOffset => DataTypeCodec.EncodeTimestampTzBinary(GetDateTimeOffset(), buffer),
            PgValueKind.Guid => DataTypeCodec.EncodeGuidBinary(ReconstructGuid(), buffer),
            PgValueKind.ByteArray => DataTypeCodec.EncodeByteArrayBinary((byte[])_objectValue!, buffer),
            PgValueKind.Object => EncodeObjectBinary(buffer),
            _ => 0
        };
    }

    private int EncodeStringBinary(Span<byte> buffer, DataType? targetDataType)
    {
        var str = (string)_objectValue!;
        var effectiveType = targetDataType ?? _dataType;
        return effectiveType.Id switch
        {
            DataTypeId.Jsonb => DataTypeCodec.EncodeJsonbBinary(str, buffer),
            _ => DataTypeCodec.EncodeStringBinary(str, buffer)
        };
    }

    private int EncodeObjectBinary(Span<byte> buffer)
    {
        return _objectValue switch
        {
            Point p => DataTypeCodec.EncodePointBinary(p, buffer),
            Line l => DataTypeCodec.EncodeLineBinary(l, buffer),
            LineSegment ls => DataTypeCodec.EncodeLineSegmentBinary(ls, buffer),
            Box b => DataTypeCodec.EncodeBoxBinary(b, buffer),
            Circle c => DataTypeCodec.EncodeCircleBinary(c, buffer),
            Data.Path path => DataTypeCodec.EncodePathBinary(path, buffer),
            Polygon poly => DataTypeCodec.EncodePolygonBinary(poly, buffer),
            Interval i => DataTypeCodec.EncodeIntervalBinary(i, buffer),
            Inet inet => DataTypeCodec.EncodeInetBinary(inet, buffer),
            Cidr cidr => DataTypeCodec.EncodeCidrBinary(cidr, buffer),
            // Array types
            bool[] ba => DataTypeCodec.EncodeBoolArrayBinary(ba, buffer),
            short[] sa => DataTypeCodec.EncodeInt16ArrayBinary(sa, buffer),
            int[] ia => DataTypeCodec.EncodeInt32ArrayBinary(ia, buffer),
            long[] la => DataTypeCodec.EncodeInt64ArrayBinary(la, buffer),
            float[] fa => DataTypeCodec.EncodeFloatArrayBinary(fa, buffer),
            double[] da => DataTypeCodec.EncodeDoubleArrayBinary(da, buffer),
            string[] stra => DataTypeCodec.EncodeStringArrayBinary(stra, buffer),
            DateOnly[] doa => DataTypeCodec.EncodeDateArrayBinary(doa, buffer),
            DateTime[] dta => DataTypeCodec.EncodeDateTimeArrayBinary(dta, buffer),
            DateTimeOffset[] dtoa => DataTypeCodec.EncodeDateTimeOffsetArrayBinary(dtoa, buffer),
            Guid[] ga => DataTypeCodec.EncodeGuidArrayBinary(ga, buffer),
            _ => DataTypeCodec.EncodeStringBinary(_objectValue?.ToString() ?? "", buffer)
        };
    }

    #endregion

    #region Implicit Conversions

    public static implicit operator PgValue(bool value) => new(value);
    public static implicit operator PgValue(short value) => new(value);
    public static implicit operator PgValue(int value) => new(value);
    public static implicit operator PgValue(long value) => new(value);
    public static implicit operator PgValue(float value) => new(value);
    public static implicit operator PgValue(double value) => new(value);
    public static implicit operator PgValue(decimal value) => new(value);
    public static implicit operator PgValue(string? value) => new(value);
    public static implicit operator PgValue(DateOnly value) => new(value);
    public static implicit operator PgValue(TimeOnly value) => new(value);
    public static implicit operator PgValue(DateTime value) => new(value);
    public static implicit operator PgValue(DateTimeOffset value) => new(value);
    public static implicit operator PgValue(Guid value) => new(value);
    public static implicit operator PgValue(byte[]? value) => new(value);

    #endregion

    public override string ToString()
    {
        return _kind == PgValueKind.Null ? "NULL" : GetString() ?? "NULL";
    }
}

/// <summary>
/// Discriminator for the type of value stored in a PgValue.
/// </summary>
public enum PgValueKind : byte
{
    Null = 0,
    Bool,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    Decimal,
    String,
    DateOnly,
    TimeOnly,
    DateTime,
    DateTimeOffset,
    Guid,
    ByteArray,
    Object // For complex types like Point, Interval, arrays, etc.
}
