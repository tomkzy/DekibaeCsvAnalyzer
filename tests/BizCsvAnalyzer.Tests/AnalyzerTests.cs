using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Xunit;
using BizCsvAnalyzer.Domain;
using BizCsvAnalyzer.Models;
using BizCsvAnalyzer.Services;

namespace BizCsvAnalyzer.Tests
{
public class AnalyzerTests
{
    [Fact]
    public async Task RunAsync_WritesAllOutputs()
    {
        var outRoot = Path.Combine(Path.GetTempPath(), "BizCsvAnalyzer_Out_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(outRoot);

        var cond = new ConditionSet
        {
            IC = "NG1ISL001",
            LotNo = "24081234",
            ClusterRadius = 3.0,
            ClusterTimeWindow = TimeSpan.FromSeconds(60),
            AlarmWindow = TimeSpan.FromSeconds(60),
            AlarmThreshold = 1,
        };

        var src = Generate();
        using var lf = LoggerFactory.Create(b => { });
        var analyzer = new Analyzer(lf.CreateLogger<Analyzer>());
        var result = await analyzer.RunAsync(src, cond, outRoot, CancellationToken.None);

        Assert.True(File.Exists(result.AggregatePath));
        Assert.True(File.Exists(result.ClusterPath));
        Assert.True(File.Exists(result.AlarmPath));

        try { Directory.Delete(outRoot, true); } catch { }
    }

    private static async IAsyncEnumerable<InspectionRecord> Generate()
    {
        var baseTime = new DateTime(2024, 01, 01, 12, 30, 0);
        yield return new InspectionRecord { LotNo = "24081234", Timestamp = baseTime, EquipmentCode = "NG1ISL001", LedgerNo = "LD01", X = 10, Y = 10, Severity = 5, CodeRaw = "01_Kizu" };
        await Task.Yield();
        yield return new InspectionRecord { LotNo = "24081234", Timestamp = baseTime.AddSeconds(10), EquipmentCode = "NG1ISL001", LedgerNo = "LD01", X = 11, Y = 11, Severity = 6, CodeRaw = "02_Ibutsu" };
        await Task.Yield();
        yield return new InspectionRecord { LotNo = "24081234", Timestamp = baseTime.AddSeconds(70), EquipmentCode = "NG1ISL001", LedgerNo = "LD01", X = 100, Y = 100, Severity = 2, CodeRaw = "02_Ibutsu" };
    }
}
}
