// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Data;

/// <summary>
/// Finite line segment data type in Postgres represented by pairs of Points that are the endpoints of the segment.
/// </summary>
public sealed class LineSegment : IEquatable<LineSegment>
{
    public Point P1 { get; set; }
    public Point P2 { get; set; }

    public LineSegment() : this(new Point(), new Point()) { }

    public LineSegment(Point p1, Point p2)
    {
        P1 = p1;
        P2 = p2;
    }

    public bool Equals(LineSegment? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return P1.Equals(other.P1) && P2.Equals(other.P2);
    }

    public override bool Equals(object? obj) => Equals(obj as LineSegment);

    public override int GetHashCode() => HashCode.Combine(P1, P2);

    public override string ToString() => $"LineSegment[{P1},{P2}]";
}
