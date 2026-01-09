// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Net;
using System.Net.Sockets;

namespace Vertx.PgClient.Data;

/// <summary>
/// A PostgreSQL classless internet domain routing (CIDR).
/// </summary>
public sealed class Cidr
{
    private IPAddress? _address;
    private int? _netmask;

    public IPAddress? Address
    {
        get => _address;
        set
        {
            if (value is not null && value.AddressFamily != AddressFamily.InterNetwork && value.AddressFamily != AddressFamily.InterNetworkV6)
            {
                throw new ArgumentException("Invalid IP address type", nameof(value));
            }
            _address = value;
        }
    }

    public int? Netmask
    {
        get => _netmask;
        set
        {
            if (value is not null && _address is not null)
            {
                var maxNetmask = _address.AddressFamily == AddressFamily.InterNetwork ? 32 : 128;
                if (value < 0 || value > maxNetmask)
                {
                    throw new ArgumentException($"Invalid netmask: {value}", nameof(value));
                }
            }
            _netmask = value;
        }
    }

    public Cidr SetAddress(IPAddress? address)
    {
        Address = address;
        return this;
    }

    public Cidr SetNetmask(int? netmask)
    {
        Netmask = netmask;
        return this;
    }

    public override string ToString() => Netmask.HasValue ? $"{Address}/{Netmask}" : $"{Address}";
}
