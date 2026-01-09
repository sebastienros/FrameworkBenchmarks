// Copyright (C) 2018 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient;

/// <summary>
/// The different values for the sslmode parameter provide different levels of
/// protection. See more information in PostgreSQL documentation for
/// "Protection Provided in Different Modes".
/// </summary>
public enum SslMode
{
    /// <summary>
    /// Only try a non-SSL connection.
    /// </summary>
    Disable,

    /// <summary>
    /// First try a non-SSL connection; if that fails, try an SSL connection.
    /// </summary>
    Allow,

    /// <summary>
    /// First try an SSL connection; if that fails, try a non-SSL connection.
    /// </summary>
    Prefer,

    /// <summary>
    /// Only try an SSL connection. If a root CA file is present, verify the certificate 
    /// in the same way as if verify-ca was specified.
    /// </summary>
    Require,

    /// <summary>
    /// Only try an SSL connection, and verify that the server certificate is issued 
    /// by a trusted certificate authority (CA).
    /// </summary>
    VerifyCa,

    /// <summary>
    /// Only try an SSL connection, verify that the server certificate is issued by a 
    /// trusted CA and that the requested server host name matches that in the certificate.
    /// </summary>
    VerifyFull
}

public static class SslModeExtensions
{
    private static readonly Dictionary<string, SslMode> ValueMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["disable"] = SslMode.Disable,
        ["allow"] = SslMode.Allow,
        ["prefer"] = SslMode.Prefer,
        ["require"] = SslMode.Require,
        ["verify-ca"] = SslMode.VerifyCa,
        ["verify-full"] = SslMode.VerifyFull
    };

    public static SslMode Parse(string value)
    {
        if (ValueMap.TryGetValue(value, out var mode))
        {
            return mode;
        }
        throw new ArgumentException($"Could not find an appropriate SSL mode for the value [{value}].");
    }

    public static string ToValue(this SslMode mode) => mode switch
    {
        SslMode.Disable => "disable",
        SslMode.Allow => "allow",
        SslMode.Prefer => "prefer",
        SslMode.Require => "require",
        SslMode.VerifyCa => "verify-ca",
        SslMode.VerifyFull => "verify-full",
        _ => throw new ArgumentOutOfRangeException(nameof(mode))
    };
}
