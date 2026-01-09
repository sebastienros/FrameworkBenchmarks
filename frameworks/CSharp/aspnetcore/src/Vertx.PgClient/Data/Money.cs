// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Globalization;

namespace Vertx.PgClient.Data;

/// <summary>
/// The PostgreSQL MONEY type.
/// <para>
/// <see cref="BigDecimalValue"/> returns the value without loss of information.
/// <see cref="DoubleValue"/> returns the value with possible loss of information.
/// </para>
/// </summary>
public readonly struct Money : IEquatable<Money>
{
    private readonly decimal _value;

    public Money(decimal value)
    {
        var absValue = Math.Abs(value);
        if (absValue > long.MaxValue / 100m)
        {
            throw new ArgumentException($"Value is too big: {value}", nameof(value));
        }
        
        // Check for more than 2 decimal digits
        var truncated = Math.Truncate(value * 100) / 100;
        if (value != truncated)
        {
            throw new ArgumentException($"Value has more than two decimal digits: {value}", nameof(value));
        }
        
        _value = value;
    }

    public Money(double value) : this((decimal)value) { }

    public Money(long value) : this((decimal)value) { }

    /// <summary>
    /// Returns the monetary amount as a decimal without loss of information.
    /// </summary>
    public decimal BigDecimalValue => _value;

    /// <summary>
    /// Returns the monetary amount as a double with possible loss of information.
    /// </summary>
    public double DoubleValue => (double)_value;

    public bool Equals(Money other) => _value == other._value;

    public override bool Equals(object? obj) => obj is Money other && Equals(other);

    public override int GetHashCode() => _value.GetHashCode();

    public static bool operator ==(Money left, Money right) => left.Equals(right);

    public static bool operator !=(Money left, Money right) => !left.Equals(right);

    public override string ToString() => $"Money({_value.ToString("0.##", CultureInfo.InvariantCulture)})";
}
