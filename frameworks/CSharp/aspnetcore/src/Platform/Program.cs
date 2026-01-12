// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace PlatformBenchmarks;

public class Program
{
    public static string[] Args;
    private static CancellationTokenSource _telemetryCts;

    public static async Task Main(string[] args)
    {
        Args = args;

        Console.WriteLine(Encoding.UTF8.GetString(BenchmarkApplication.ApplicationName));
        Console.WriteLine(Encoding.UTF8.GetString(BenchmarkApplication.Paths.Plaintext));
        Console.WriteLine(Encoding.UTF8.GetString(BenchmarkApplication.Paths.Json));
        Console.WriteLine(Encoding.UTF8.GetString(BenchmarkApplication.Paths.FortunesRaw));
        Console.WriteLine(Encoding.UTF8.GetString(BenchmarkApplication.Paths.SingleQuery));
        Console.WriteLine(Encoding.UTF8.GetString(BenchmarkApplication.Paths.Updates));
        Console.WriteLine(Encoding.UTF8.GetString(BenchmarkApplication.Paths.MultipleQueries));
        DateHeader.SyncDateTimer();

        var host = BuildWebHost(args);
        var config = (IConfiguration)host.Services.GetService(typeof(IConfiguration));

        try
        {
            await BenchmarkApplication.RawDb.PopulateCache();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error trying to populate database cache: {ex}");
        }

        // Start telemetry background task
    #if PGCLIENT
        _telemetryCts = new CancellationTokenSource();
        _ = Task.Run(() => PrintTelemetryAsync(_telemetryCts.Token));
    #endif

        await host.RunAsync();
        
        _telemetryCts?.Cancel();
    }

#if PGCLIENT
    private static async Task PrintTelemetryAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(5000, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            
            var pool = BenchmarkApplication.RawDb?.Pool;
            if (pool == null) continue;
            
            var sb = new StringBuilder();
            sb.AppendLine();
            sb.AppendLine("=== Pool Telemetry ===");
            sb.AppendLine($"Pool Size: {pool.Size}/{pool.Options.MaxSize}");
            
            var connCount = pool.MultiplexedConnectionCount;
            if (connCount > 0)
            {
                sb.AppendLine($"Multiplexed Connections: {connCount}");
                int i = 0;
                foreach (var (inflight, limit, available) in pool.GetMultiplexedConnectionStats())
                {
                    sb.AppendLine($"  Conn[{i}]: Inflight={inflight}/{limit}, Available={available}");
                    i++;
                }
            }
            sb.AppendLine("======================");
            
            Console.Write(sb.ToString());
        }
    }
#endif

    public static IHost BuildWebHost(string[] args)
    {
        Console.WriteLine($"BuildWebHost()");
        Console.WriteLine($"Args: {string.Join(' ', args)}");

        var config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
#if DEBUG
            .AddUserSecrets<Program>()
#endif
            .AddEnvironmentVariables()
            .AddCommandLine(args)
            .Build();

        var appSettings = config.Get<AppSettings>();
        Console.WriteLine($"ConnectionString: {appSettings.ConnectionString}");

        BenchmarkApplication.RawDb = new RawDb(appSettings);

        var hostBuilder = Host.CreateDefaultBuilder(args)
            .ConfigureWebHost(webHostBuilder =>
            {
                webHostBuilder
                    .UseBenchmarksConfiguration(config)
                    .UseKestrel((context, options) =>
                    {
                        var endPoint = context.Configuration.CreateIPEndPoint();

                        options.Listen(endPoint, builder =>
                        {
                            builder.UseHttpApplication<BenchmarkApplication>();
                        });
                    })
                    .UseStartup<Startup>();

                webHostBuilder.UseSockets(options =>
                {
                    options.WaitForDataBeforeAllocatingBuffer = false;

                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    {
                        options.UnsafePreferInlineScheduling = true;
                    }
                });
            });

        var host = hostBuilder.Build();

        return host;
    }
}
