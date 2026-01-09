// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Net;

namespace Vertx.PgClient.Data;

/// <summary>
/// A PostgreSQL inet network address.
/// </summary>
public sealed class Inet
{
    private int? _netmask;

    /// <summary>
    /// Gets or sets the inet address.
    /// </summary>
    public IPAddress? Address { get; set; }

    /// <summary>
    /// Gets or sets the optional netmask.
    /// </summary>
    public int? Netmask
    {
        get => _netmask;
        set
        {
            if (value is not null && (value < 0 || value > 255))
            {
                throw new ArgumentException($"Invalid netmask: {value}", nameof(value));
            }
            _netmask = value;
        }
    }

    public Inet SetAddress(IPAddress? address)
    {
        Address = address;
        return this;
    }

    public Inet SetNetmask(int? netmask)
    {
        Netmask = netmask;
        return this;
    }

    public override string ToString() => Netmask.HasValue ? $"{Address}/{Netmask}" : $"{Address}";
}
