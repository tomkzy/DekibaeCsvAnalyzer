using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Xunit;
using DekibaeCsvAnalyzer.Domain;
using DekibaeCsvAnalyzer.Models;
using DekibaeCsvAnalyzer.Services;

namespace BizCsvAnalyzer.Tests
{
    public class AlarmThresholdTests
    {
        [Fact]
        public async Task Alarm_IsOne_WhenCountEqualsThreshold()
        {
            var outRoot = Path.Combine(Path.GetTempPath(), "BizCsvAnalyzer_Alarm_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(outRoot);

            try
            {
                // 条件: 同一ウィンドウ(60秒)内に2件、閾値=2 → 以上(>=)なら Alarm=1
                var cond = new ConditionSet
                {
                    IC = "NG1ISL001",
                    LotNo = "24081234",
                    ClusterRadius = 3.0,
                    ClusterTimeWindow = TimeSpan.FromSeconds(60),
                    AlarmWindow = TimeSpan.FromSeconds(60),
                    AlarmThreshold = 2,
                };

                var baseTime = new DateTime(2024, 01, 01, 12, 30, 0, DateTimeKind.Local);
                var records = new List<InspectionRecord>
                {
                    new InspectionRecord { LotNo = "24081234", Timestamp = baseTime.AddSeconds(1),  EquipmentCode = "NG1ISL001", LedgerNo = "LD01", X = 10, Y = 10, Severity = 5, CodeRaw = "01_Kizu" },
                    new InspectionRecord { LotNo = "24081234", Timestamp = baseTime.AddSeconds(30), EquipmentCode = "NG1ISL001", LedgerNo = "LD01", X = 11, Y = 11, Severity = 6, CodeRaw = "02_Ibutsu" },
                };

                async IAsyncEnumerable<InspectionRecord> Source()
                {
                    foreach (var r in records)
                    {
                        yield return r;
                        await Task.Yield();
                    }
                }

                using var lf = LoggerFactory.Create(b => { });
                var analyzer = new Analyzer(lf.CreateLogger<Analyzer>());
                var result = await analyzer.RunAsync(Source(), cond, outRoot, CancellationToken.None);

                Assert.True(File.Exists(result.AlarmPath));
                var lines = File.ReadAllLines(result.AlarmPath);
                Assert.True(lines.Length >= 2); // header + 1
                var cols = lines[1].Split(',');
                Assert.Equal("2", cols[2]); // Count
                Assert.Equal("2", cols[3]); // Threshold
                Assert.Equal("1", cols[4]); // Alarm (=1 when Count == Threshold)
            }
            finally
            {
                try { Directory.Delete(outRoot, true); } catch { }
            }
        }
    }
}

