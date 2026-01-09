// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Text;

namespace Vertx.PgClient.Data;

/// <summary>
/// Polygon data type in Postgres represented by lists of points (the vertexes of the polygon).
/// Polygons are very similar to closed paths, but are stored differently and have their own set of support routines.
/// </summary>
public sealed class Polygon : IEquatable<Polygon>
{
    public List<Point> Points { get; set; }

    public Polygon() : this(new List<Point>()) { }

    public Polygon(List<Point> points)
    {
        Points = points ?? throw new ArgumentNullException(nameof(points));
    }

    public Polygon AddPoint(Point point)
    {
        Points.Add(point);
        return this;
    }

    public bool Equals(Polygon? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return Points.SequenceEqual(other.Points);
    }

    public override bool Equals(object? obj) => Equals(obj as Polygon);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var point in Points)
        {
            hash.Add(point);
        }
        return hash.ToHashCode();
    }

    public override string ToString()
    {
        var sb = new StringBuilder("Polygon(");
        for (int i = 0; i < Points.Count; i++)
        {
            sb.Append(Points[i]);
            if (i != Points.Count - 1)
            {
                sb.Append(',');
            }
        }
        sb.Append(')');
        return sb.ToString();
    }
}
