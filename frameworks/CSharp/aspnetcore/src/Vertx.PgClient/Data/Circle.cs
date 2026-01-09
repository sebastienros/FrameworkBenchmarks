// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Data;

/// <summary>
/// Circle data type in Postgres represented by a center Point and radius.
/// </summary>
public sealed class Circle : IEquatable<Circle>
{
    public Point CenterPoint { get; set; }
    public double Radius { get; set; }

    public Circle() : this(new Point(), 0.0) { }

    public Circle(Point centerPoint, double radius)
    {
        CenterPoint = centerPoint;
        Radius = radius;
    }

    public bool Equals(Circle? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return CenterPoint.Equals(other.CenterPoint) && Radius == other.Radius;
    }

    public override bool Equals(object? obj) => Equals(obj as Circle);

    public override int GetHashCode() => HashCode.Combine(CenterPoint, Radius);

    public override string ToString() => $"Circle<{CenterPoint},{Radius}>";
}
