// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Text;

namespace Vertx.PgClient.Data;

/// <summary>
/// Path data type in Postgres represented by lists of connected points.
/// Paths can be open, where the first and last points in the list are considered not connected,
/// or closed, where the first and last points are considered connected.
/// </summary>
public sealed class Path : IEquatable<Path>
{
    public bool IsOpen { get; set; }
    public List<Point> Points { get; set; }

    public Path() : this(false, new List<Point>()) { }

    public Path(bool isOpen, List<Point> points)
    {
        IsOpen = isOpen;
        Points = points ?? throw new ArgumentNullException(nameof(points));
    }

    public Path AddPoint(Point point)
    {
        Points.Add(point);
        return this;
    }

    public bool Equals(Path? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return IsOpen == other.IsOpen && Points.SequenceEqual(other.Points);
    }

    public override bool Equals(object? obj) => Equals(obj as Path);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(IsOpen);
        foreach (var point in Points)
        {
            hash.Add(point);
        }
        return hash.ToHashCode();
    }

    public override string ToString()
    {
        var left = IsOpen ? "[" : "(";
        var right = IsOpen ? "]" : ")";
        var sb = new StringBuilder($"Path{left}");
        for (int i = 0; i < Points.Count; i++)
        {
            sb.Append(Points[i]);
            if (i != Points.Count - 1)
            {
                sb.Append(',');
            }
        }
        sb.Append(right);
        return sb.ToString();
    }
}
