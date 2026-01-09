// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Codec;

/// <summary>
/// Data format for PostgreSQL wire protocol.
/// </summary>
public enum DataFormat
{
    Text = 0,
    Binary = 1
}

internal static class DataFormatExtensions
{
    public static DataFormat ValueOf(int id) => id == 0 ? DataFormat.Text : DataFormat.Binary;
}
