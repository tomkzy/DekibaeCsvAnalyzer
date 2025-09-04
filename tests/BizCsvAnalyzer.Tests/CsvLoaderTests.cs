using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Xunit;
using BizCsvAnalyzer.Services;

namespace BizCsvAnalyzer.Tests
{
public class CsvLoaderTests
{
    [Fact]
    public async Task LoadAsync_StreamsRecords_AndSkipsBadLines()
    {
        var tmp = Path.Combine(Path.GetTempPath(), "BizCsvAnalyzer_TestCSV_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tmp);
        var path = Path.Combine(tmp, "test.csv");

        await File.WriteAllTextAsync(path, string.Join(Environment.NewLine, new[]
        {
            "LotNo,Timestamp,EquipmentCode,LedgerNo,X,Y,Severity,CodeRaw",
            "24081234,20240101-123001,NG1ISL001,LD01,10.0,11.0,5,01_Kizu",
            "24081234,20240101-1230XX,NG1ISL001,LD01,10.0,11.0,5,01_Kizu", // 壊れたタイムスタンプ → スキップ
            "24081234,20240101-123002,NG1ISL001,LD01,12.0,13.0,7,02_Ibutsu",
        }));

        using var lf = LoggerFactory.Create(b => b.AddConsole());
        var loader = new CsvLoader(lf.CreateLogger<CsvLoader>());

        var list = await loader.LoadAsync(path, CancellationToken.None).ToListAsync();
        Assert.Equal(2, list.Count);
        Assert.Equal("01_Kizu", list[0].CodeRaw);
        Assert.Equal(7, list[1].Severity);

        try { Directory.Delete(tmp, true); } catch { }
    }
}

internal static class AsyncEnumerableExt
{
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var x in source) list.Add(x);
        return list;
    }
}
}
