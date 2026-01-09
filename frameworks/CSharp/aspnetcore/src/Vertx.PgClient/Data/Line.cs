// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Data;

/// <summary>
/// Line data type in Postgres represented by the linear equation Ax + By + C = 0, where A and B are not both zero.
/// </summary>
public sealed class Line : IEquatable<Line>
{
    private double _a;
    private double _b;
    private double _c;

    public Line(double a, double b, double c)
    {
        Validate(a, b);
        _a = a;
        _b = b;
        _c = c;
    }

    public double A
    {
        get => _a;
        set
        {
            Validate(value, _b);
            _a = value;
        }
    }

    public double B
    {
        get => _b;
        set
        {
            Validate(_a, value);
            _b = value;
        }
    }

    public double C
    {
        get => _c;
        set => _c = value;
    }

    private static void Validate(double a, double b)
    {
        if (a == 0.0 && b == 0.0)
        {
            throw new ArgumentException("Invalid line specification: A and B cannot both be zero");
        }
    }

    public bool Equals(Line? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return _a == other._a && _b == other._b && _c == other._c;
    }

    public override bool Equals(object? obj) => Equals(obj as Line);

    public override int GetHashCode() => HashCode.Combine(_a, _b, _c);

    public override string ToString() => $"{{{_a},{_b},{_c}}}";
}
