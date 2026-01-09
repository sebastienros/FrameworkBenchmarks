// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient;

/// <summary>
/// A mutable tuple for building parameter values.
/// </summary>
public sealed class Tuple
{
    private Tuple()
    {
        _values = new ();
    }

    private Tuple(List<PgValue> values)
    {
        _values = values;
    }

    private readonly List<PgValue> _values;

    public int Size => _values.Count;

    public Tuple AddValue<T>(T? value)
    {
        _values.Add(PgValue.From(value));
        return this;
    }

    public Tuple AddValue(PgValue value)
    {
        _values.Add(value);
        return this;
    }

    public PgValue this[int position] 
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(position, nameof(position));
            
            if (position >= _values.Count)
            {
                return PgValue.Null;
            }

            return _values[position];
        }

        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(position, nameof(position));

            if (position >= _values.Count)
            {
                while (_values.Count <= position)
                {
                    _values.Add(PgValue.Null);
                }
            }
            _values[position] = value;
        }
    }

    public static Tuple Create(params IEnumerable<PgValue> values)
    {
        return new Tuple([.. values]);
    }

    public static Tuple Create(params IEnumerable<object?> values)
    {
        var tuple = new Tuple();
        foreach (var value in values)
        {
            tuple.AddValue(value);
        }
        return tuple;
    }

    public static Tuple Create<T>(T? value)
    {
        return new Tuple([PgValue.From(value)]);
    }
}
