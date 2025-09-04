using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using BizCsvAnalyzer.Domain;
using BizCsvAnalyzer.Models;

/*
  例外/ロギング/キャンセル方針
  - 例外: 入出力(ディレクトリ作成/書込)や致命的エラーは上位へ再スロー。処理継続可能な個別レコード不整合はWARNログしてスキップ。
  - ロギング: 入力件数、クラスタ/アラーム出力件数、生成ファイルパスをINFO; スキップやメモリ制限の影響をWARN。
  - キャンセル: ストリーミングループ中にCancellationTokenを監視し即時中断。出力ストリームは finally でクローズ。
  - メモリ: 近傍クラスタリングはグリッド+時間窓スライドでO(n)近似。古いアンカーを定期的に破棄。
*/

namespace BizCsvAnalyzer.Services
{
    public sealed class Analyzer
    {
        private readonly ILogger<Analyzer> _logger;
        public Analyzer(ILogger<Analyzer> logger) { _logger = logger; }

        public sealed class AnalyzerResult
        {
            public AnalyzerResult(string aggregatePath, string clusterPath, string alarmPath)
            {
                AggregatePath = aggregatePath; ClusterPath = clusterPath; AlarmPath = alarmPath;
            }
            public string AggregatePath { get; }
            public string ClusterPath { get; }
            public string AlarmPath { get; }
        }

        public async Task<AnalyzerResult> RunAsync(
            IAsyncEnumerable<InspectionRecord> source,
            ConditionSet conditions,
            string outputRoot,
            CancellationToken ct = default(CancellationToken))
        {
        conditions.Validate();
        if (conditions.HasErrors) throw new InvalidOperationException("ConditionSet が不正です。");

        var outputsDir = Path.Combine(outputRoot, "exports");
        Directory.CreateDirectory(outputsDir);

        string icName = conditions.IC ?? "IC";
        string lotName = conditions.LotNo ?? "Lot";
        var tsStamp = DateTime.Now.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);

        var aggregatePath = Path.Combine(outputsDir, $"Aggregate_{icName}_{lotName}_{tsStamp}.csv");
        var clusterPath = Path.Combine(outputsDir, $"Cluster_{icName}_{lotName}_{tsStamp}.csv");
        var alarmPath = Path.Combine(outputsDir, $"AlarmRate_{icName}_{lotName}_{tsStamp}.csv");

        await using var aggWriter = new StreamWriter(aggregatePath, false, new System.Text.UTF8Encoding(true));
        await using var cluWriter = new StreamWriter(clusterPath, false, new System.Text.UTF8Encoding(true));
        await using var almWriter = new StreamWriter(alarmPath, false, new System.Text.UTF8Encoding(true));

        await cluWriter.WriteLineAsync("LotNo,Timestamp,EquipmentCode,LedgerNo,X,Y,Severity,CodeRaw,ClusterId");

        var byCode = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        long total = 0;

        var alarmCounts = new SortedDictionary<long, int>();
        long alarmWinTicks = conditions.AlarmWindow.Ticks;

        var clusterer = new OnlineClusterer(conditions.ClusterRadius, conditions.ClusterTimeWindow, _logger);

        InspectionRecord? first = null;
        try
        {
            var e = source.GetAsyncEnumerator(ct);
            try
            {
                while (await e.MoveNextAsync())
                {
                    var r = e.Current;
                    if (!Match(r, conditions)) continue;

                    if (first == null) first = r;
                    icName = !string.IsNullOrEmpty(conditions.IC) ? conditions.IC : (first != null ? first.EquipmentCode : icName);
                    lotName = !string.IsNullOrEmpty(conditions.LotNo) ? conditions.LotNo : (first != null ? first.LotNo : lotName);

                    total++;
                    var key = r.CodeRaw ?? string.Empty;
                    int c;
                    byCode[key] = byCode.TryGetValue(key, out c) ? c + 1 : 1;

                    var clusterId = clusterer.AssignCluster(r);
                    await cluWriter.WriteLineAsync(string.Join(',', new[]
                    {
                        Csv(r.LotNo), r.Timestamp.ToString("yyyy-MM-dd HH:mm:ss"), Csv(r.EquipmentCode), Csv(r.LedgerNo),
                        r.X.ToString(CultureInfo.InvariantCulture), r.Y.ToString(CultureInfo.InvariantCulture), r.Severity.ToString(CultureInfo.InvariantCulture),
                        Csv(r.CodeRaw), clusterId.ToString(CultureInfo.InvariantCulture)
                    }));

                    var ws = (r.Timestamp.Ticks / alarmWinTicks) * alarmWinTicks; // バケット開始Ticks
                    int ac;
                    alarmCounts[ws] = alarmCounts.TryGetValue(ws, out ac) ? ac + 1 : 1;

                    if (total % 10000 == 0) clusterer.Prune(r.Timestamp);
                }
            }
            finally { if (e != null) await e.DisposeAsync(); }
        }
        finally
        {
            clusterer.Dispose();
        }

        // Aggregate 出力
        await aggWriter.WriteLineAsync("Code,Count,Ratio");
        foreach (var kv in byCode.OrderByDescending(k => k.Value))
        {
            var ratio = total > 0 ? (double)kv.Value / total : 0;
            await aggWriter.WriteLineAsync(string.Join(',', Csv(kv.Key), kv.Value.ToString(CultureInfo.InvariantCulture), ratio.ToString("0.#####", CultureInfo.InvariantCulture)));
        }

        // Alarm 出力
        await almWriter.WriteLineAsync("WindowStart,WindowEnd,Count,Threshold,Alarm");
        foreach (var kv in alarmCounts)
        {
            var start = new DateTime(kv.Key, DateTimeKind.Local);
            var end = start.Add(conditions.AlarmWindow);
            var count = kv.Value;
            var alarm = count > conditions.AlarmThreshold ? 1 : 0; // “超” → strictly greater
            await almWriter.WriteLineAsync(string.Join(',',
                start.ToString("yyyy-MM-dd HH:mm:ss"), end.ToString("yyyy-MM-dd HH:mm:ss"),
                count.ToString(CultureInfo.InvariantCulture), conditions.AlarmThreshold.ToString(CultureInfo.InvariantCulture), alarm.ToString(CultureInfo.InvariantCulture)));
        }

        _logger.LogInformation("集計:{Agg} クラスタ:{Clu} アラーム:{Alm}", aggregatePath, clusterPath, alarmPath);
        return new AnalyzerResult(aggregatePath, clusterPath, alarmPath);
    }

    private static string Csv(string s)
    {
        if (s == null) s = string.Empty;
        if (s.Contains('"') || s.Contains(',') || s.Contains('\n'))
            return '"' + s.Replace("\"", "\"\"") + '"';
        return s;
    }

    private static bool Match(InspectionRecord r, ConditionSet c)
    {
        if (c.From.HasValue && r.Timestamp < c.From.Value) return false;
        if (c.To.HasValue && r.Timestamp > c.To.Value) return false;
        if (!string.IsNullOrWhiteSpace(c.IC) && !string.Equals(r.EquipmentCode, c.IC, StringComparison.OrdinalIgnoreCase)) return false;
        if (!string.IsNullOrWhiteSpace(c.LotNo) && !string.Equals(r.LotNo, c.LotNo, StringComparison.OrdinalIgnoreCase)) return false;
        if (!string.IsNullOrWhiteSpace(c.EquipmentCode) && !string.Equals(r.EquipmentCode, c.EquipmentCode, StringComparison.OrdinalIgnoreCase)) return false;
        if (c.SeverityMin.HasValue && r.Severity < c.SeverityMin.Value) return false;
        if (!string.IsNullOrWhiteSpace(c.CodeFilter) && (r.CodeRaw == null || r.CodeRaw.IndexOf(c.CodeFilter, StringComparison.OrdinalIgnoreCase) < 0)) return false;
        return true;
    }

    private sealed class OnlineClusterer : IDisposable
    {
        private readonly double _r;
        private readonly double _r2;
        private readonly TimeSpan _tw;
        private readonly ILogger _logger;
        private readonly Dictionary<Tuple<int, int>, List<Anchor>> _grid = new Dictionary<Tuple<int, int>, List<Anchor>>();
        private int _nextId = 1;

        private sealed class Anchor
        {
            public int Id;
            public double X;
            public double Y;
            public DateTime Last;
        }

        public OnlineClusterer(double radius, TimeSpan timeWindow, ILogger logger)
        {
            _r = radius;
            _r2 = radius * radius;
            _tw = timeWindow;
            _logger = logger;
        }

        private static Tuple<int, int> Cell(double x, double y, double r)
        {
            return Tuple.Create((int)Math.Floor(x / r), (int)Math.Floor(y / r));
        }

        public int AssignCluster(InspectionRecord r)
        {
            var cell = Cell(r.X, r.Y, _r);
            var now = r.Timestamp;
            int chosen = 0;
            Anchor chosenA = null;
            for (var dx = -1; dx <= 1 && chosen == 0; dx++)
            {
                for (var dy = -1; dy <= 1 && chosen == 0; dy++)
                {
                    var key = Tuple.Create(cell.Item1 + dx, cell.Item2 + dy);
                    List<Anchor> list;
                    if (!_grid.TryGetValue(key, out list)) continue;
                    for (var i = list.Count - 1; i >= 0; i--)
                    {
                        var a = list[i];
                        if (now - a.Last > _tw) { list.RemoveAt(i); continue; }
                        var ddx = a.X - r.X; var ddy = a.Y - r.Y;
                        var d2 = ddx * ddx + ddy * ddy;
                        if (d2 <= _r2)
                        {
                            chosen = a.Id; chosenA = a; break;
                        }
                    }
                }
            }

            if (chosen == 0)
            {
                var a = new Anchor { Id = _nextId++, X = r.X, Y = r.Y, Last = now };
                List<Anchor> list;
                if (!_grid.TryGetValue(cell, out list)) { list = new List<Anchor>(); _grid[cell] = list; }
                list.Add(a);
                chosen = a.Id; chosenA = a;
            }

            if (chosenA != null) chosenA.Last = now;
            return chosen;
        }

        public void Prune(DateTime now)
        {
            var removed = 0;
            foreach (var kv in _grid)
            {
                var list = kv.Value;
                for (var i = list.Count - 1; i >= 0; i--)
                {
                    if (now - list[i].Last > _tw) { list.RemoveAt(i); removed++; }
                }
            }
            if (removed > 0) _logger.LogDebug("古いクラスタ破棄: {Count}", removed);
        }

        public void Dispose()
        {
            _grid.Clear();
        }
    }
    }
}
