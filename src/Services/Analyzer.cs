using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using DekibaeCsvAnalyzer.Domain;
using DekibaeCsvAnalyzer.Models;

/*
  ロギング/キャンセル方針
  - 例外: 入出力(ディレクトリ作成/書込み)の致命的エラーは上位へ再スロー。
           継続可能な個別レコード不整合は WARN ログしてスキップ。
  - ロギング: 入力件数、クラスタ/アラーム出力件数、生成ファイルパスを INFO。
  - キャンセル: ストリーミング中は CancellationToken を監視し即時中断。
  - メモリ: 近傍クラスタリングはグリッド + 時間窓スライドで近似。古いアンカーは定期的に破棄。
*/

namespace DekibaeCsvAnalyzer.Services
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
            CancellationToken ct = default)
        {
            conditions.Validate();
            if (conditions.HasErrors) throw new InvalidOperationException("ConditionSet is invalid.");

            var outputsDir = Path.Combine(outputRoot, "exports");
            Directory.CreateDirectory(outputsDir);

            string icLabel = string.IsNullOrWhiteSpace(conditions.IC) ? "IC" : conditions.IC;
            string lotLabel = string.IsNullOrWhiteSpace(conditions.LotNo) ? "ALL" : conditions.LotNo;
            string dateLabel = (conditions.From ?? DateTime.MinValue).ToString("yyyyMMdd", CultureInfo.InvariantCulture)
                              + "-" + (conditions.To ?? conditions.From ?? DateTime.MinValue).ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            var tsStamp = DateTime.Now.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);

            string? aggregatePath = null;
            string? clusterPath = null;
            string? alarmPath = null;
            StreamWriter? aggWriter = null;
            StreamWriter? cluWriter = null;
            StreamWriter? almWriter = null;

            async Task EnsureWritersAsync(string? effectiveIc)
            {
                if (aggWriter != null) return;
                var icForName = string.IsNullOrWhiteSpace(conditions.IC)
                    ? (string.IsNullOrWhiteSpace(effectiveIc) ? icLabel : effectiveIc)
                    : icLabel;
                aggregatePath = Path.Combine(outputsDir, $"Aggregate_{icForName}_{lotLabel}_{dateLabel}_{tsStamp}.csv");
                clusterPath   = Path.Combine(outputsDir, $"Cluster_{icForName}_{lotLabel}_{dateLabel}_{tsStamp}.csv");
                alarmPath     = Path.Combine(outputsDir, $"AlarmRate_{icForName}_{lotLabel}_{dateLabel}_{tsStamp}.csv");

                aggWriter = new StreamWriter(aggregatePath, false, new System.Text.UTF8Encoding(true));
                cluWriter = new StreamWriter(clusterPath,   false, new System.Text.UTF8Encoding(true));
                almWriter = new StreamWriter(alarmPath,     false, new System.Text.UTF8Encoding(true));

                await cluWriter.WriteLineAsync("LotNo,Timestamp,EquipmentCode,LedgerNo,Face,X,Y,Severity,CodeRaw,ClusterId");
            }

            var byCode = new Dictionary<string, Dictionary<string, int>>(StringComparer.OrdinalIgnoreCase); // Face -> Code -> Count
            var faceTotals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
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
                        ct.ThrowIfCancellationRequested();
                        var r = e.Current;
                        if (!Match(r, conditions)) continue;

                        if (first == null) first = r;
                        await EnsureWritersAsync(first.EquipmentCode);
                        var clw = cluWriter!;

                        total++;
                        var face = r.Face ?? string.Empty;
                        var code = r.CodeRaw ?? string.Empty;
                        if (!byCode.TryGetValue(face, out var dict)) { dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase); byCode[face] = dict; }
                        int c;
                        dict[code] = dict.TryGetValue(code, out c) ? c + 1 : 1;
                        int ft;
                        faceTotals[face] = faceTotals.TryGetValue(face, out ft) ? ft + 1 : 1;

                        var clusterId = clusterer.AssignCluster(r);
                        await clw.WriteLineAsync(string.Join(',', new[]
                        {
                            Csv(r.LotNo), r.Timestamp.ToString("yyyy-MM-dd HH:mm:ss"), Csv(r.EquipmentCode), Csv(r.LedgerNo), Csv(r.Face ?? string.Empty),
                            r.X.ToString(CultureInfo.InvariantCulture), r.Y.ToString(CultureInfo.InvariantCulture), r.Severity.ToString(CultureInfo.InvariantCulture),
                            Csv(r.CodeRaw), clusterId.ToString(CultureInfo.InvariantCulture)
                        }));

                        // アラーム判定用の時間窓バケット開始Ticksを計算
                        var ws = (r.Timestamp.Ticks / alarmWinTicks) * alarmWinTicks;
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

            // Aggregate/Alarm 出力
            if (aggWriter == null || almWriter == null || cluWriter == null)
            {
                await EnsureWritersAsync(first?.EquipmentCode);
            }
            var agw = aggWriter!;
            var alw = almWriter!;
            await agw.WriteLineAsync("Face,Code,Count,RatioInFace");

            foreach (var faceEntry in byCode)
            {
                var face = faceEntry.Key ?? string.Empty;
                int ft = 0; faceTotals.TryGetValue(face, out ft);
                foreach (var kv in faceEntry.Value.OrderByDescending(k => k.Value))
                {
                    var ratio = ft > 0 ? (double)kv.Value / ft : 0;
                    await agw.WriteLineAsync(string.Join(',', Csv(face), Csv(kv.Key), kv.Value.ToString(CultureInfo.InvariantCulture), ratio.ToString("0.#####", CultureInfo.InvariantCulture)));
                }
            }

            // Alarm 出力
            await alw.WriteLineAsync("WindowStart,WindowEnd,Count,Threshold,Alarm");
            foreach (var kv in alarmCounts)
            {
                var start = new DateTime(kv.Key, DateTimeKind.Local);
                var end = start.Add(conditions.AlarmWindow);
                var count = kv.Value;
                // アラーム判定は「閾値以上(>=)」
                var alarm = (count >= conditions.AlarmThreshold) ? 1 : 0;
                await alw.WriteLineAsync(string.Join(',',
                    start.ToString("yyyy-MM-dd HH:mm:ss"),
                    end.ToString("yyyy-MM-dd HH:mm:ss"),
                    count.ToString(CultureInfo.InvariantCulture),
                    conditions.AlarmThreshold.ToString(CultureInfo.InvariantCulture),
                    alarm.ToString(CultureInfo.InvariantCulture)));
            }

            await agw.FlushAsync();
            await alw.FlushAsync();
            await cluWriter!.FlushAsync();
            agw.Dispose(); alw.Dispose(); cluWriter.Dispose();

            return new AnalyzerResult(aggregatePath!, clusterPath!, alarmPath!);
        }

        private static string Csv(string? s)
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
                Anchor? chosenA = null;
                for (var dx = -1; dx <= 1 && chosen == 0; dx++)
                {
                    for (var dy = -1; dy <= 1 && chosen == 0; dy++)
                    {
                        var key = Tuple.Create(cell.Item1 + dx, cell.Item2 + dy);
                        if (!_grid.TryGetValue(key, out var list)) continue;
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
                    if (!_grid.TryGetValue(cell, out var list)) { list = new List<Anchor>(); _grid[cell] = list; }
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
                if (removed > 0) _logger.LogDebug("古いクラスターアンカーを削除 {Count}", removed);
            }

            public void Dispose()
            {
                _grid.Clear();
            }
        }
    }
}

