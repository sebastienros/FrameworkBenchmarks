// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Data;

/// <summary>
/// Postgres Interval is date and time based such as 120 years 3 months 332 days 20 hours 20 minutes 20.999999 seconds.
/// </summary>
public sealed class Interval : IEquatable<Interval>
{
    public int Years { get; set; }
    public int Months { get; set; }
    public int Days { get; set; }
    public int Hours { get; set; }
    public int Minutes { get; set; }
    public int Seconds { get; set; }
    public int Microseconds { get; set; }

    public Interval() : this(0, 0, 0, 0, 0, 0, 0) { }

    public Interval(int years, int months = 0, int days = 0, int hours = 0, int minutes = 0, int seconds = 0, int microseconds = 0)
    {
        Years = years;
        Months = months;
        Days = days;
        Hours = hours;
        Minutes = minutes;
        Seconds = seconds;
        Microseconds = microseconds;
    }

    public static Interval Of() => new();

    public static Interval Of(int years, int months = 0, int days = 0, int hours = 0, int minutes = 0, int seconds = 0, int microseconds = 0)
        => new(years, months, days, hours, minutes, seconds, microseconds);

    /// <summary>
    /// Creates an instance from the given TimeSpan.
    /// The conversion algorithm assumes a year lasts 12 months and a month lasts 30 days,
    /// as Postgres does and ISO 8601 suggests.
    /// </summary>
    public static Interval Of(TimeSpan duration)
    {
        long totalSeconds = (long)duration.TotalSeconds;

        int years = (int)(totalSeconds / 31104000);
        long remainder = totalSeconds % 31104000;

        int months = (int)(remainder / 2592000);
        remainder = totalSeconds % 2592000;

        int days = (int)(remainder / 86400);
        remainder %= 86400;

        int hours = (int)(remainder / 3600);
        remainder %= 3600;

        int minutes = (int)(remainder / 60);
        remainder %= 60;

        int microseconds = (int)((duration.Ticks % TimeSpan.TicksPerSecond) / 10);

        return new Interval(years, months, days, hours, minutes, (int)remainder, microseconds);
    }

    public Interval WithYears(int years) { Years = years; return this; }
    public Interval WithMonths(int months) { Months = months; return this; }
    public Interval WithDays(int days) { Days = days; return this; }
    public Interval WithHours(int hours) { Hours = hours; return this; }
    public Interval WithMinutes(int minutes) { Minutes = minutes; return this; }
    public Interval WithSeconds(int seconds) { Seconds = seconds; return this; }
    public Interval WithMicroseconds(int microseconds) { Microseconds = microseconds; return this; }

    public bool Equals(Interval? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return Years == other.Years &&
               Months == other.Months &&
               Days == other.Days &&
               Hours == other.Hours &&
               Minutes == other.Minutes &&
               Seconds == other.Seconds &&
               Microseconds == other.Microseconds;
    }

    public override bool Equals(object? obj) => Equals(obj as Interval);

    public override int GetHashCode() => HashCode.Combine(Years, Months, Days, Hours, Minutes, Seconds, Microseconds);

    public override string ToString() =>
        $"Interval( {Years} years {Months} months {Days} days {Hours} hours {Minutes} minutes {Seconds} seconds {Microseconds} microseconds )";

    /// <summary>
    /// Convert this interval to a TimeSpan.
    /// The conversion algorithm assumes a year lasts 12 months and a month lasts 30 days,
    /// as Postgres does and ISO 8601 suggests.
    /// </summary>
    public TimeSpan ToTimeSpan()
    {
        long totalSeconds = ((((Years * 12L + Months) * 30L + Days) * 24L + Hours) * 60 + Minutes) * 60 + Seconds;
        return TimeSpan.FromSeconds(totalSeconds) + TimeSpan.FromTicks(Microseconds * 10);
    }
}
