// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Data;

/// <summary>
/// A PostgreSQL point.
/// </summary>
public readonly struct Point : IEquatable<Point>
{
    public double X { get; init; }
    public double Y { get; init; }

    public Point() : this(0, 0) { }

    public Point(double x, double y)
    {
        X = x;
        Y = y;
    }

    public bool Equals(Point other) => X == other.X && Y == other.Y;

    public override bool Equals(object? obj) => obj is Point other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(X, Y);

    public static bool operator ==(Point left, Point right) => left.Equals(right);

    public static bool operator !=(Point left, Point right) => !left.Equals(right);

    public override string ToString() => $"Point({X},{Y})";
}
