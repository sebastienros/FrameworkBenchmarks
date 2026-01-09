// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using Vertx.PgClient.Data;

namespace Vertx.PgClient.Codec;

/// <summary>
/// PostgreSQL object identifiers (OIDs) for data types.
/// </summary>
public enum DataTypeId
{
    Bool = 16,
    BoolArray = 1000,
    Int2 = 21,
    Int2Array = 1005,
    Int4 = 23,
    Int4Array = 1007,
    Int8 = 20,
    Int8Array = 1016,
    Float4 = 700,
    Float4Array = 1021,
    Float8 = 701,
    Float8Array = 1022,
    Numeric = 1700,
    NumericArray = 1231,
    Money = 790,
    MoneyArray = 791,
    Bit = 1560,
    BitArray = 1561,
    Varbit = 1562,
    VarbitArray = 1563,
    Char = 18,
    CharArray = 1002,
    Varchar = 1043,
    VarcharArray = 1015,
    Bpchar = 1042,
    BpcharArray = 1014,
    Text = 25,
    TextArray = 1009,
    Name = 19,
    NameArray = 1003,
    Date = 1082,
    DateArray = 1182,
    Time = 1083,
    TimeArray = 1183,
    Timetz = 1266,
    TimetzArray = 1270,
    Timestamp = 1114,
    TimestampArray = 1115,
    Timestamptz = 1184,
    TimestamptzArray = 1185,
    Interval = 1186,
    IntervalArray = 1187,
    Bytea = 17,
    ByteaArray = 1001,
    MacAddr = 829,
    Inet = 869,
    InetArray = 1041,
    Cidr = 650,
    MacAddr8 = 774,
    Uuid = 2950,
    UuidArray = 2951,
    Json = 114,
    JsonArray = 199,
    Jsonb = 3802,
    JsonbArray = 3807,
    Xml = 142,
    XmlArray = 143,
    Point = 600,
    PointArray = 1017,
    Line = 628,
    LineArray = 629,
    Lseg = 601,
    LsegArray = 1018,
    Box = 603,
    BoxArray = 1020,
    Path = 602,
    PathArray = 1019,
    Polygon = 604,
    PolygonArray = 1027,
    Circle = 718,
    CircleArray = 719,
    Hstore = 33670,
    Oid = 26,
    OidArray = 1028,
    Void = 2278,
    Unknown = 705,
    TsVector = 3614,
    TsVectorArray = 3643,
    TsQuery = 3615,
    TsQueryArray = 3645
}

/// <summary>
/// Data type information for PostgreSQL columns.
/// </summary>
/// <remarks>
/// This class is deeply immutable so it can be cached safely.
/// </remarks>
public sealed class DataType
{
    private static readonly Dictionary<int, DataType> OidToDataType = new();
    private static readonly Dictionary<Type, DataType> ClrTypeToDataType = new();

    public DataTypeId Id { get; }
    public bool SupportsBinary { get; }
    public Type ClrType { get; }
    public bool IsArray { get; }
    public DataType? ElementType { get; }

    private DataType(DataTypeId id, bool supportsBinary, Type clrType, bool isArray = false, DataType? elementType = null)
    {
        Id = id;
        SupportsBinary = supportsBinary;
        ClrType = clrType;
        IsArray = isArray;
        ElementType = elementType;
    }

    public static DataType ValueOf(int oid)
    {
        if (OidToDataType.TryGetValue(oid, out var dataType))
        {
            return dataType;
        }
        return Unknown;
    }

    /// <summary>
    /// Alias for ValueOf - looks up a data type by its PostgreSQL OID.
    /// </summary>
    public static DataType LookupByOid(int oid) => ValueOf(oid);

    public static DataType Lookup(Type type)
    {
        if (ClrTypeToDataType.TryGetValue(type, out var dataType))
        {
            return dataType;
        }
        if (typeof(byte[]).IsAssignableFrom(type))
        {
            return Bytea;
        }
        return Unknown;
    }

    // Scalar types
    public static readonly DataType Bool = new(DataTypeId.Bool, true, typeof(bool));
    public static readonly DataType Int2 = new(DataTypeId.Int2, true, typeof(short));
    public static readonly DataType Int4 = new(DataTypeId.Int4, true, typeof(int));
    public static readonly DataType Int8 = new(DataTypeId.Int8, true, typeof(long));
    public static readonly DataType Float4 = new(DataTypeId.Float4, true, typeof(float));
    public static readonly DataType Float8 = new(DataTypeId.Float8, true, typeof(double));
    public static readonly DataType Numeric = new(DataTypeId.Numeric, false, typeof(decimal));
    public static readonly DataType MoneyType = new(DataTypeId.Money, true, typeof(Money));
    public static readonly DataType Char = new(DataTypeId.Char, true, typeof(string));
    public static readonly DataType Varchar = new(DataTypeId.Varchar, true, typeof(string));
    public static readonly DataType Bpchar = new(DataTypeId.Bpchar, true, typeof(string));
    public static readonly DataType Text = new(DataTypeId.Text, true, typeof(string));
    public static readonly DataType Name = new(DataTypeId.Name, true, typeof(string));
    public static readonly DataType Date = new(DataTypeId.Date, true, typeof(DateOnly));
    public static readonly DataType Time = new(DataTypeId.Time, true, typeof(TimeOnly));
    public static readonly DataType Timetz = new(DataTypeId.Timetz, true, typeof(DateTimeOffset));
    public static readonly DataType Timestamp = new(DataTypeId.Timestamp, true, typeof(DateTime));
    public static readonly DataType Timestamptz = new(DataTypeId.Timestamptz, true, typeof(DateTimeOffset));
    public static readonly DataType IntervalType = new(DataTypeId.Interval, true, typeof(Interval));
    public static readonly DataType Bytea = new(DataTypeId.Bytea, true, typeof(byte[]));
    public static readonly DataType InetType = new(DataTypeId.Inet, true, typeof(Inet));
    public static readonly DataType CidrType = new(DataTypeId.Cidr, true, typeof(Cidr));
    public static readonly DataType Uuid = new(DataTypeId.Uuid, true, typeof(Guid));
    public static readonly DataType Json = new(DataTypeId.Json, true, typeof(string));
    public static readonly DataType Jsonb = new(DataTypeId.Jsonb, true, typeof(string));
    public static readonly DataType PointType = new(DataTypeId.Point, true, typeof(Point));
    public static readonly DataType LineType = new(DataTypeId.Line, true, typeof(Line));
    public static readonly DataType Lseg = new(DataTypeId.Lseg, true, typeof(LineSegment));
    public static readonly DataType BoxType = new(DataTypeId.Box, true, typeof(Box));
    public static readonly DataType PathType = new(DataTypeId.Path, true, typeof(Data.Path));
    public static readonly DataType PolygonType = new(DataTypeId.Polygon, true, typeof(Polygon));
    public static readonly DataType CircleType = new(DataTypeId.Circle, true, typeof(Circle));
    public static readonly DataType Unknown = new(DataTypeId.Unknown, false, typeof(string));
    public static readonly DataType Void = new(DataTypeId.Void, true, typeof(object));

    // Array types
    public static readonly DataType BoolArray = new(DataTypeId.BoolArray, true, typeof(bool[]), true, Bool);
    public static readonly DataType Int2Array = new(DataTypeId.Int2Array, true, typeof(short[]), true, Int2);
    public static readonly DataType Int4Array = new(DataTypeId.Int4Array, true, typeof(int[]), true, Int4);
    public static readonly DataType Int8Array = new(DataTypeId.Int8Array, true, typeof(long[]), true, Int8);
    public static readonly DataType Float4Array = new(DataTypeId.Float4Array, true, typeof(float[]), true, Float4);
    public static readonly DataType Float8Array = new(DataTypeId.Float8Array, true, typeof(double[]), true, Float8);
    public static readonly DataType NumericArray = new(DataTypeId.NumericArray, false, typeof(decimal[]), true, Numeric);
    public static readonly DataType VarcharArray = new(DataTypeId.VarcharArray, true, typeof(string[]), true, Varchar);
    public static readonly DataType TextArray = new(DataTypeId.TextArray, true, typeof(string[]), true, Text);
    public static readonly DataType DateArray = new(DataTypeId.DateArray, true, typeof(DateOnly[]), true, Date);
    public static readonly DataType TimestampArray = new(DataTypeId.TimestampArray, true, typeof(DateTime[]), true, Timestamp);
    public static readonly DataType TimestamptzArray = new(DataTypeId.TimestamptzArray, true, typeof(DateTimeOffset[]), true, Timestamptz);
    public static readonly DataType ByteaArray = new(DataTypeId.ByteaArray, true, typeof(byte[][]), true, Bytea);
    public static readonly DataType UuidArray = new(DataTypeId.UuidArray, true, typeof(Guid[]), true, Uuid);

    static DataType()
    {
        // Register all data types
        RegisterType(Bool);
        RegisterType(Int2);
        RegisterType(Int4);
        RegisterType(Int8);
        RegisterType(Float4);
        RegisterType(Float8);
        RegisterType(Numeric);
        RegisterType(MoneyType);
        RegisterType(Char);
        RegisterType(Varchar);
        RegisterType(Bpchar);
        RegisterType(Text);
        RegisterType(Name);
        RegisterType(Date);
        RegisterType(Time);
        RegisterType(Timetz);
        RegisterType(Timestamp);
        RegisterType(Timestamptz);
        RegisterType(IntervalType);
        RegisterType(Bytea);
        RegisterType(InetType);
        RegisterType(CidrType);
        RegisterType(Uuid);
        RegisterType(Json);
        RegisterType(Jsonb);
        RegisterType(PointType);
        RegisterType(LineType);
        RegisterType(Lseg);
        RegisterType(BoxType);
        RegisterType(PathType);
        RegisterType(PolygonType);
        RegisterType(CircleType);
        RegisterType(Unknown);
        RegisterType(Void);

        // Array types
        RegisterType(BoolArray);
        RegisterType(Int2Array);
        RegisterType(Int4Array);
        RegisterType(Int8Array);
        RegisterType(Float4Array);
        RegisterType(Float8Array);
        RegisterType(NumericArray);
        RegisterType(VarcharArray);
        RegisterType(TextArray);
        RegisterType(DateArray);
        RegisterType(TimestampArray);
        RegisterType(TimestamptzArray);
        RegisterType(ByteaArray);
        RegisterType(UuidArray);

        // CLR type mappings
        ClrTypeToDataType[typeof(string)] = Varchar;
        ClrTypeToDataType[typeof(bool)] = Bool;
        ClrTypeToDataType[typeof(short)] = Int2;
        ClrTypeToDataType[typeof(int)] = Int4;
        ClrTypeToDataType[typeof(long)] = Int8;
        ClrTypeToDataType[typeof(float)] = Float4;
        ClrTypeToDataType[typeof(double)] = Float8;
        ClrTypeToDataType[typeof(decimal)] = Numeric;
        ClrTypeToDataType[typeof(DateOnly)] = Date;
        ClrTypeToDataType[typeof(DateTime)] = Timestamp;
        ClrTypeToDataType[typeof(DateTimeOffset)] = Timestamptz;
        ClrTypeToDataType[typeof(TimeOnly)] = Time;
        ClrTypeToDataType[typeof(Guid)] = Uuid;
        ClrTypeToDataType[typeof(byte[])] = Bytea;
        ClrTypeToDataType[typeof(Point)] = PointType;
        ClrTypeToDataType[typeof(Line)] = LineType;
        ClrTypeToDataType[typeof(LineSegment)] = Lseg;
        ClrTypeToDataType[typeof(Box)] = BoxType;
        ClrTypeToDataType[typeof(Data.Path)] = PathType;
        ClrTypeToDataType[typeof(Polygon)] = PolygonType;
        ClrTypeToDataType[typeof(Circle)] = CircleType;
        ClrTypeToDataType[typeof(Interval)] = IntervalType;
        ClrTypeToDataType[typeof(Money)] = MoneyType;
        ClrTypeToDataType[typeof(Inet)] = InetType;
        ClrTypeToDataType[typeof(Cidr)] = CidrType;
        
        // Array CLR type mappings
        ClrTypeToDataType[typeof(bool[])] = BoolArray;
        ClrTypeToDataType[typeof(short[])] = Int2Array;
        ClrTypeToDataType[typeof(int[])] = Int4Array;
        ClrTypeToDataType[typeof(long[])] = Int8Array;
        ClrTypeToDataType[typeof(float[])] = Float4Array;
        ClrTypeToDataType[typeof(double[])] = Float8Array;
        ClrTypeToDataType[typeof(decimal[])] = NumericArray;
        ClrTypeToDataType[typeof(string[])] = TextArray;
        ClrTypeToDataType[typeof(DateOnly[])] = DateArray;
        ClrTypeToDataType[typeof(DateTime[])] = TimestampArray;
        ClrTypeToDataType[typeof(DateTimeOffset[])] = TimestamptzArray;
        ClrTypeToDataType[typeof(Guid[])] = UuidArray;
    }

    private static void RegisterType(DataType type)
    {
        OidToDataType[(int)type.Id] = type;
    }
}
