// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Security.Cryptography;
using System.Text;

namespace Vertx.PgClient;

/// <summary>
/// SCRAM-SHA-256 authentication client implementation.
/// Implements RFC 5802 (SCRAM) and RFC 7677 (SCRAM-SHA-256) for PostgreSQL.
/// </summary>
internal sealed class ScramClient
{
    private const string ScramSha256 = "SCRAM-SHA-256";
    private const string ScramSha256Plus = "SCRAM-SHA-256-PLUS";
    private const int NonceLength = 24;

    private readonly string _username;
    private readonly string _password;
    private readonly byte[]? _channelBindingData;

    private string? _clientNonce;
    private string? _serverNonce;
    private byte[]? _salt;
    private int _iterations;
    private string? _clientFirstMessageBare;
    private string? _serverFirstMessage;
    private byte[]? _authMessage;
    private string? _mechanism;

    public ScramClient(string username, string password, byte[]? channelBindingData = null)
    {
        _username = username;
        _password = password;
        _channelBindingData = channelBindingData;
    }

    /// <summary>
    /// Gets the selected SCRAM mechanism name.
    /// </summary>
    public string Mechanism => _mechanism ?? ScramSha256;

    /// <summary>
    /// Selects the best mechanism from the server's advertised mechanisms.
    /// </summary>
    public bool SelectMechanism(string[] mechanisms)
    {
        // Prefer channel binding if available
        if (_channelBindingData is { Length: > 0 } && mechanisms.Contains(ScramSha256Plus))
        {
            _mechanism = ScramSha256Plus;
            return true;
        }

        if (mechanisms.Contains(ScramSha256))
        {
            _mechanism = ScramSha256;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Creates the client-first-message for SCRAM authentication.
    /// </summary>
    public string CreateClientFirstMessage()
    {
        _clientNonce = GenerateNonce();

        // gs2-header: channel binding flag and authzid
        // n = no channel binding, y = client supports but not used, p = channel binding used
        var gs2Header = _mechanism == ScramSha256Plus ? "p=tls-server-end-point,," : "n,,";

        // Normalize username per RFC 5802 (SASLprep, but PostgreSQL uses simple rules)
        var saslName = NormalizeSaslName(_username);

        // client-first-message-bare: n=<username>,r=<nonce>
        _clientFirstMessageBare = $"n={saslName},r={_clientNonce}";

        // client-first-message: gs2-header + client-first-message-bare
        return gs2Header + _clientFirstMessageBare;
    }

    /// <summary>
    /// Processes the server-first-message and creates the client-final-message.
    /// </summary>
    public string ProcessServerFirstMessage(string serverFirstMessage)
    {
        _serverFirstMessage = serverFirstMessage;

        // Parse server-first-message: r=<nonce>,s=<salt>,i=<iterations>
        var parts = ParseMessage(serverFirstMessage);

        if (!parts.TryGetValue("r", out var serverNonce))
            throw new PgException("SCRAM: server-first-message missing nonce", "28000", "authentication_failed");

        if (!parts.TryGetValue("s", out var saltBase64))
            throw new PgException("SCRAM: server-first-message missing salt", "28000", "authentication_failed");

        if (!parts.TryGetValue("i", out var iterationsStr) || !int.TryParse(iterationsStr, out _iterations))
            throw new PgException("SCRAM: server-first-message missing iterations", "28000", "authentication_failed");

        // Verify the server nonce starts with our client nonce
        if (!serverNonce.StartsWith(_clientNonce!))
            throw new PgException("SCRAM: server nonce doesn't contain client nonce", "28000", "authentication_failed");

        _serverNonce = serverNonce;
        _salt = Convert.FromBase64String(saltBase64);

        // Build client-final-message
        var channelBinding = BuildChannelBinding();
        var clientFinalMessageWithoutProof = $"c={channelBinding},r={_serverNonce}";

        // AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
        _authMessage = Encoding.UTF8.GetBytes($"{_clientFirstMessageBare},{_serverFirstMessage},{clientFinalMessageWithoutProof}");

        // Compute proof
        var saltedPassword = ComputeSaltedPassword();
        var clientKey = ComputeHmac(saltedPassword, "Client Key"u8);
        var storedKey = SHA256.HashData(clientKey);
        var clientSignature = ComputeHmac(storedKey, _authMessage);
        var clientProof = XorBytes(clientKey, clientSignature);

        return $"{clientFinalMessageWithoutProof},p={Convert.ToBase64String(clientProof)}";
    }

    /// <summary>
    /// Verifies the server-final-message.
    /// </summary>
    public void VerifyServerFinalMessage(string serverFinalMessage)
    {
        var parts = ParseMessage(serverFinalMessage);

        // Check for error
        if (parts.TryGetValue("e", out var error))
            throw new PgException($"SCRAM: server error: {error}", "28000", "authentication_failed");

        if (!parts.TryGetValue("v", out var serverSignatureBase64))
            throw new PgException("SCRAM: server-final-message missing verifier", "28000", "authentication_failed");

        // Verify server signature
        var saltedPassword = ComputeSaltedPassword();
        var serverKey = ComputeHmac(saltedPassword, "Server Key"u8);
        var expectedSignature = ComputeHmac(serverKey, _authMessage!);

        var serverSignature = Convert.FromBase64String(serverSignatureBase64);
        if (!CryptographicOperations.FixedTimeEquals(serverSignature, expectedSignature))
            throw new PgException("SCRAM: server signature verification failed", "28000", "authentication_failed");
    }

    private string BuildChannelBinding()
    {
        // gs2-header: same as in client-first-message
        byte[] gs2Header;
        if (_mechanism == ScramSha256Plus)
        {
            gs2Header = Encoding.UTF8.GetBytes("p=tls-server-end-point,,");
            // Append channel binding data
            var combined = new byte[gs2Header.Length + (_channelBindingData?.Length ?? 0)];
            gs2Header.CopyTo(combined, 0);
            _channelBindingData?.CopyTo(combined, gs2Header.Length);
            return Convert.ToBase64String(combined);
        }
        else
        {
            gs2Header = "n,,"u8.ToArray();
            return Convert.ToBase64String(gs2Header);
        }
    }

    private byte[] ComputeSaltedPassword()
    {
        // SaltedPassword = Hi(Normalize(password), salt, i)
        // Hi is PBKDF2 with HMAC-SHA-256
        var normalizedPassword = Encoding.UTF8.GetBytes(NormalizePassword(_password));
        return Rfc2898DeriveBytes.Pbkdf2(normalizedPassword, _salt!, _iterations, HashAlgorithmName.SHA256, 32);
    }

    private static byte[] ComputeHmac(byte[] key, ReadOnlySpan<byte> data)
    {
        return HMACSHA256.HashData(key, data);
    }

    private static byte[] XorBytes(byte[] a, byte[] b)
    {
        var result = new byte[a.Length];
        for (int i = 0; i < a.Length; i++)
            result[i] = (byte)(a[i] ^ b[i]);
        return result;
    }

    private static Dictionary<string, string> ParseMessage(string message)
    {
        var result = new Dictionary<string, string>();
        ReadOnlySpan<char> span = message.AsSpan();
        
        foreach (var range in span.Split(','))
        {
            var part = span[range];
            // Find '=' in this part
            int eq = part.IndexOf('=');
            if (eq > 0)
            {
                var key = part[..eq].ToString();
                var value = part[(eq + 1)..].ToString();
                result[key] = value;
            }
        }
        
        return result;
    }

    private static string NormalizeSaslName(string name)
    {
        // RFC 5802: Replace '=' with '=3D' and ',' with '=2C'
        return name.Replace("=", "=3D").Replace(",", "=2C");
    }

    private static string NormalizePassword(string password)
    {
        // PostgreSQL uses SASLprep (RFC 4013) but with some simplifications
        // For now, just return the password as-is
        // A full implementation would need proper Unicode normalization
        return password;
    }

    private static string GenerateNonce()
    {
        var bytes = RandomNumberGenerator.GetBytes(NonceLength);
        // Use URL-safe base64 without padding (RFC 5802 allows printable ASCII except comma)
        return Convert.ToBase64String(bytes).Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }
}
