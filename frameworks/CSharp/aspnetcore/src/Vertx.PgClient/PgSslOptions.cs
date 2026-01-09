// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Security.Cryptography.X509Certificates;

namespace Vertx.PgClient;

/// <summary>
/// SSL/TLS options for PostgreSQL connections.
/// </summary>
public sealed class PgSslOptions
{
    /// <summary>
    /// Gets or sets whether to enable SSL/TLS.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the client certificate for authentication.
    /// </summary>
    public X509Certificate2? ClientCertificate { get; set; }

    /// <summary>
    /// Gets or sets the collection of trusted CA certificates.
    /// </summary>
    public X509Certificate2Collection? TrustCertificates { get; set; }

    /// <summary>
    /// Gets or sets the hostname verification algorithm.
    /// Common values: "HTTPS", "" (empty to disable).
    /// </summary>
    public string? HostnameVerificationAlgorithm { get; set; }

    /// <summary>
    /// Gets or sets whether to trust all certificates (for development only!).
    /// </summary>
    public bool TrustAll { get; set; }

    public PgSslOptions() { }

    public PgSslOptions(PgSslOptions other)
    {
        Enabled = other.Enabled;
        ClientCertificate = other.ClientCertificate;
        TrustCertificates = other.TrustCertificates;
        HostnameVerificationAlgorithm = other.HostnameVerificationAlgorithm;
        TrustAll = other.TrustAll;
    }

    public PgSslOptions Copy() => new(this);
}
