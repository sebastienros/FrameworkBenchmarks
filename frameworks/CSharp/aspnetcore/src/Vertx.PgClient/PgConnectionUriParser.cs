// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

using System.Text.RegularExpressions;
using System.Web;

namespace Vertx.PgClient;

/// <summary>
/// Parser for PostgreSQL connection URIs.
/// Based on PostgreSQL 11: postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&amp;...]
/// </summary>
internal static partial class PgConnectionUriParser
{
    private const string SchemeDesignatorRegex = @"postgre(s|sql)://";
    private const string UserInfoRegex = @"((?<userinfo>[a-zA-Z0-9\-._~%!*]+(:[a-zA-Z0-9\-._~%!*]+)?)@)?";
    private const string NetLocationRegex = @"(?<netloc>[0-9.]+|\[[a-zA-Z0-9:]+\]|[a-zA-Z0-9\-._~%]+)?";
    private const string PortRegex = @"(:(?<port>\d+))?";
    private const string DatabaseRegex = @"(/(?<database>[a-zA-Z0-9\-._~%!*]+))?";
    private const string ParamsRegex = @"(\?(?<params>.*))?";

    private static readonly Regex SchemeDesignatorPattern = new($"^{SchemeDesignatorRegex}", RegexOptions.Compiled);
    
    private static readonly Regex FullUriPattern = new(
        $"^{SchemeDesignatorRegex}{UserInfoRegex}{NetLocationRegex}{PortRegex}{DatabaseRegex}{ParamsRegex}$",
        RegexOptions.Compiled);

    public static PgConnectOptions Parse(string connectionUri)
    {
        return Parse(connectionUri, exact: true);
    }

    public static PgConnectOptions Parse(string connectionUri, bool exact)
    {
        try
        {
            var matcher = SchemeDesignatorPattern.Match(connectionUri);
            if (matcher.Success || exact)
            {
                var options = new PgConnectOptions();
                DoParse(connectionUri, options);
                return options;
            }
            else
            {
                throw new ArgumentException($"Cannot parse invalid connection URI: {connectionUri}");
            }
        }
        catch (Exception e) when (e is not ArgumentException)
        {
            throw new ArgumentException($"Cannot parse invalid connection URI: {connectionUri}", e);
        }
    }

    private static void DoParse(string connectionUri, PgConnectOptions options)
    {
        var match = FullUriPattern.Match(connectionUri);

        if (match.Success)
        {
            ParseUserAndPassword(match.Groups["userinfo"].Value, options);
            ParseNetLocation(match.Groups["netloc"].Value, options);
            ParsePort(match.Groups["port"].Value, options);
            ParseDatabaseName(match.Groups["database"].Value, options);
            ParseParameters(match.Groups["params"].Value, options);
        }
        else
        {
            throw new ArgumentException("Wrong syntax of connection URI");
        }
    }

    private static void ParseUserAndPassword(string? userInfo, PgConnectOptions options)
    {
        if (string.IsNullOrEmpty(userInfo))
        {
            return;
        }

        ReadOnlySpan<char> span = userInfo.AsSpan();
        int colonIndex = span.IndexOf(':');
        int lastColonIndex = span.LastIndexOf(':');
        
        if (colonIndex >= 0 && colonIndex == lastColonIndex)
        {
            // Exactly one colon
            if (colonIndex == 0)
            {
                throw new ArgumentException("Cannot only specify the password without a concrete user");
            }
            options.User = DecodeUrl(span[..colonIndex].ToString());
            options.Password = DecodeUrl(span[(colonIndex + 1)..].ToString());
        }
        else if (colonIndex < 0)
        {
            // No colon
            options.User = DecodeUrl(userInfo);
        }
        else
        {
            throw new ArgumentException("Cannot use multiple delimiters to delimit user and password");
        }
    }

    private static void ParseNetLocation(string? hostInfo, PgConnectOptions options)
    {
        if (string.IsNullOrEmpty(hostInfo))
        {
            return;
        }
        ParseNetLocationValue(DecodeUrl(hostInfo), options);
    }

    private static void ParsePort(string? portInfo, PgConnectOptions options)
    {
        if (string.IsNullOrEmpty(portInfo))
        {
            return;
        }

        if (!int.TryParse(DecodeUrl(portInfo), out var port))
        {
            throw new ArgumentException("The port must be a valid integer");
        }

        if (port > 65535 || port <= 0)
        {
            throw new ArgumentException("The port can only range in 1-65535");
        }

        options.Port = port;
    }

    private static void ParseDatabaseName(string? databaseInfo, PgConnectOptions options)
    {
        if (string.IsNullOrEmpty(databaseInfo))
        {
            return;
        }
        options.Database = DecodeUrl(databaseInfo);
    }

    private static void ParseParameters(string? parametersInfo, PgConnectOptions options)
    {
        if (string.IsNullOrEmpty(parametersInfo))
        {
            return;
        }

        ReadOnlySpan<char> span = parametersInfo.AsSpan();
        
        foreach (var range in span.Split('&'))
        {
            var parameterPair = span[range];
            
            if (parameterPair.IsEmpty)
            {
                continue;
            }

            int indexOfDelimiter = parameterPair.IndexOf('=');
            if (indexOfDelimiter < 0)
            {
                throw new ArgumentException($"Missing delimiter '=' of parameters \"{parametersInfo}\" in the part \"{parameterPair.ToString()}\"");
            }
            
            // Key needs to be lowercase, so we need a string anyway
            var key = parameterPair[..indexOfDelimiter].ToString().ToLowerInvariant();
            var value = DecodeUrl(parameterPair[(indexOfDelimiter + 1)..].Trim().ToString());

            switch (key)
            {
                case "port":
                    ParsePort(value, options);
                    break;
                case "host":
                    ParseNetLocationValue(value, options);
                    break;
                case "hostaddr":
                    options.Host = value;
                    break;
                case "user":
                    options.User = value;
                    break;
                case "password":
                    options.Password = value;
                    break;
                case "dbname":
                    options.Database = value;
                    break;
                case "sslmode":
                    options.SslMode = SslModeExtensions.Parse(value);
                    break;
                case "pipelining_limit":
                case "pipelininglimit":
                    if (int.TryParse(value, out var pipeliningLimit) && pipeliningLimit >= 1)
                    {
                        options.PipeliningLimit = pipeliningLimit;
                    }
                    break;
                case "cache_prepared_statements":
                case "cachepreparedstatements":
                    if (bool.TryParse(value, out var cachePrepared))
                    {
                        options.CachePreparedStatements = cachePrepared;
                    }
                    else if (value == "1" || value.Equals("yes", StringComparison.OrdinalIgnoreCase))
                    {
                        options.CachePreparedStatements = true;
                    }
                    else if (value == "0" || value.Equals("no", StringComparison.OrdinalIgnoreCase))
                    {
                        options.CachePreparedStatements = false;
                    }
                    break;
                case "prepared_statement_cache_max_size":
                case "preparedstatementcachemaxsize":
                    if (int.TryParse(value, out var cacheMaxSize) && cacheMaxSize >= 1)
                    {
                        options.PreparedStatementCacheMaxSize = cacheMaxSize;
                    }
                    break;
                // Pool-related parameters - ignore here (handled by PgPoolOptions.FromUri)
                case "pool_size":
                case "poolsize":
                case "pool_name":
                case "poolname":
                case "pipelined":
                case "idle_timeout":
                case "idletimeout":
                case "connection_timeout":
                case "connectiontimeout":
                case "max_lifetime":
                case "maxlifetime":
                case "max_wait_queue_size":
                case "maxwaitqueuesize":
                    // Skip - these are handled by PgPoolOptions
                    break;
                default:
                    options.Properties[key] = value;
                    break;
            }
        }
    }

    private static void ParseNetLocationValue(string hostValue, PgConnectOptions options)
    {
        if (IsRegardedAsIpv6Address(hostValue))
        {
            options.Host = hostValue[1..^1];
        }
        else
        {
            options.Host = hostValue;
        }
    }

    private static bool IsRegardedAsIpv6Address(string hostAddress)
    {
        return hostAddress.Length >= 2 && hostAddress[0] == '[' && hostAddress[^1] == ']';
    }

    private static string DecodeUrl(string url)
    {
        return HttpUtility.UrlDecode(url);
    }
}
