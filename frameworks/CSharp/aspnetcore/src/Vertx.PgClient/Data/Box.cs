// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Data;

/// <summary>
/// Rectangular box data type in Postgres represented by pairs of Points that are opposite corners of the box.
/// </summary>
public sealed class Box : IEquatable<Box>
{
    public Point UpperRightCorner { get; set; }
    public Point LowerLeftCorner { get; set; }

    public Box() : this(new Point(), new Point()) { }

    public Box(Point upperRightCorner, Point lowerLeftCorner)
    {
        UpperRightCorner = upperRightCorner;
        LowerLeftCorner = lowerLeftCorner;
    }

    public bool Equals(Box? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return UpperRightCorner.Equals(other.UpperRightCorner) && LowerLeftCorner.Equals(other.LowerLeftCorner);
    }

    public override bool Equals(object? obj) => Equals(obj as Box);

    public override int GetHashCode() => HashCode.Combine(UpperRightCorner, LowerLeftCorner);

    public override string ToString() => $"Box({UpperRightCorner},{LowerLeftCorner})";
}
