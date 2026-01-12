// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using RazorSlices;

namespace PlatformBenchmarks;

public sealed partial class BenchmarkApplication
{
    // Pre-computed static header to avoid runtime string operations
    private static ReadOnlySpan<byte> _fortunesPreamble =>
        "HTTP/1.1 200 OK\r\n"u8 +
        "Server: K\r\n"u8 +
        "Content-Type: text/html; charset=utf-8\r\n"u8 +
        "Transfer-Encoding: chunked\r\n"u8;

    // Thread-local chunked writer to avoid pool overhead
    [ThreadStatic]
    private static ChunkedPipeWriter t_chunkedWriter;

    private ValueTask FortunesRaw(PipeWriter pipeWriter)
    {
        var task = RawDb.LoadFortunesRows();

        // Fast path: avoid async state machine when task is already complete
        if (task.IsCompletedSuccessfully)
        {
            return OutputFortunes(pipeWriter, task.Result, FortunesTemplateFactory);
        }

        return FortunesRawSlowAsync(pipeWriter, task);
    }

    private async ValueTask FortunesRawSlowAsync(PipeWriter pipeWriter, Task<System.Collections.Generic.List<FortuneUtf8>> task)
    {
        await OutputFortunes(pipeWriter, await task, FortunesTemplateFactory);
    }

    private ValueTask OutputFortunes<TModel>(PipeWriter pipeWriter, TModel model, Func<TModel, RazorSlice<TModel>> templateFactory)
    {
        // Write headers with single span acquisition
        var headersLength = _fortunesPreamble.Length + DateHeader.HeaderBytes.Length;
        var headersSpan = pipeWriter.GetSpan(headersLength);
        _fortunesPreamble.CopyTo(headersSpan);
        DateHeader.HeaderBytes.CopyTo(headersSpan[_fortunesPreamble.Length..]);
        pipeWriter.Advance(headersLength);

        // Use thread-local writer instead of pool to reduce overhead
        var chunkedWriter = t_chunkedWriter ??= new ChunkedPipeWriter();
        chunkedWriter.SetOutput(pipeWriter, chunkSizeHint: 2048);

        var template = templateFactory(model);
        var renderTask = template.RenderAsync(chunkedWriter, HtmlEncoder);

        if (renderTask.IsCompletedSuccessfully)
        {
            renderTask.GetAwaiter().GetResult();
            EndTemplateRenderingInline(chunkedWriter, template);
            return ValueTask.CompletedTask;
        }

        return AwaitTemplateRenderTask(renderTask, chunkedWriter, template);
    }

    private static async ValueTask AwaitTemplateRenderTask(ValueTask renderTask, ChunkedPipeWriter chunkedWriter, RazorSlice template)
    {
        await renderTask;
        EndTemplateRenderingInline(chunkedWriter, template);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EndTemplateRenderingInline(ChunkedPipeWriter chunkedWriter, RazorSlice template)
    {
        chunkedWriter.Complete();
        // Note: No need to return to pool since we use thread-local
        template.Dispose();
    }
}
