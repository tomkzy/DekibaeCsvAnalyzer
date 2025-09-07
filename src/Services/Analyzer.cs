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
  萓句､・繝ｭ繧ｮ繝ｳ繧ｰ/繧ｭ繝｣繝ｳ繧ｻ繝ｫ譁ｹ驥・
  - 萓句､・ 蜈･蜃ｺ蜉・繝・ぅ繝ｬ繧ｯ繝医Μ菴懈・/譖ｸ霎ｼ)繧・・蜻ｽ逧・お繝ｩ繝ｼ縺ｯ荳贋ｽ阪∈蜀阪せ繝ｭ繝ｼ縲ょ・逅・ｶ咏ｶ壼庄閭ｽ縺ｪ蛟句挨繝ｬ繧ｳ繝ｼ繝我ｸ肴紛蜷医・WARN繝ｭ繧ｰ縺励※繧ｹ繧ｭ繝・・縲・
  - 繝ｭ繧ｮ繝ｳ繧ｰ: 蜈･蜉帑ｻｶ謨ｰ縲√け繝ｩ繧ｹ繧ｿ/繧｢繝ｩ繝ｼ繝蜃ｺ蜉帑ｻｶ謨ｰ縲∫函謌舌ヵ繧｡繧､繝ｫ繝代せ繧棚NFO; 繧ｹ繧ｭ繝・・繧・Γ繝｢繝ｪ蛻ｶ髯舌・蠖ｱ髻ｿ繧淡ARN縲・
  - 繧ｭ繝｣繝ｳ繧ｻ繝ｫ: 繧ｹ繝医Μ繝ｼ繝溘Φ繧ｰ繝ｫ繝ｼ繝嶺ｸｭ縺ｫCancellationToken繧堤屮隕悶＠蜊ｳ譎ゆｸｭ譁ｭ縲ょ・蜉帙せ繝医Μ繝ｼ繝縺ｯ finally 縺ｧ繧ｯ繝ｭ繝ｼ繧ｺ縲・
  - 繝｡繝｢繝ｪ: 霑大ｍ繧ｯ繝ｩ繧ｹ繧ｿ繝ｪ繝ｳ繧ｰ縺ｯ繧ｰ繝ｪ繝・ラ+譎る俣遯薙せ繝ｩ繧､繝峨〒O(n)霑台ｼｼ縲ょ商縺・い繝ｳ繧ｫ繝ｼ繧貞ｮ壽悄逧・↓遐ｴ譽・・
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
            CancellationToken ct = default(CancellationToken))
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

        string aggregatePath = null;
        string clusterPath = null;
        string alarmPath = null;
        StreamWriter aggWriter = null;
        StreamWriter cluWriter = null;
        StreamWriter almWriter = null;

        async System.Threading.Tasks.Task EnsureWritersAsync(string effectiveIc)
        {
            if (aggWriter != null) return;
            var icForName = string.IsNullOrWhiteSpace(conditions.IC) ? (string.IsNullOrWhiteSpace(effectiveIc) ? icLabel : effectiveIc) : icLabel;
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

                    var ws = (r.Timestamp.Ticks / alarmWinTicks) * alarmWinTicks; // 繝舌こ繝・ヨ髢句ｧ亀icks
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

        // Aggregate 蜃ｺ蜉・
        if (aggWriter == null || almWriter == null || cluWriter == null)
        {
            await EnsureWritersAsync(null);
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

        // Alarm 蜃ｺ蜉・
        await alw.WriteLineAsync("WindowStart,WindowEnd,Count,Threshold,Alarm");
        foreach (var kv in alarmCounts)
        {
            var start = new DateTime(kv.Key, DateTimeKind.Local);
            var end = start.Add(conditions.AlarmWindow);
            var count = kv.Value;
            var alarm = count > conditions.AlarmThreshold ? 1 : 0; // 窶懆ｶ・・竊・strictly greater
            // 髢ｾ蛟､莉･荳・>=)縺ｫ邨ｱ荳・医さ繝｡繝ｳ繝医→螳溯｣・・荳堺ｸ閾ｴ繧呈弍豁｣・・            alarm = (count >= conditions.AlarmThreshold) ? 1 : 0;
            await alw.WriteLineAsync(string.Join(',',
                start.ToString("yyyy-MM-dd HH:mm:ss"), end.ToString("yyyy-MM-dd HH:mm:ss"),
                count.ToString(CultureInfo.InvariantCulture), conditions.AlarmThreshold.ToString(CultureInfo.InvariantCulture), alarm.ToString(CultureInfo.InvariantCulture)));
        }

        _logger.LogInformation("髮・ｨ・{Agg} 繧ｯ繝ｩ繧ｹ繧ｿ:{Clu} 繧｢繝ｩ繝ｼ繝:{Alm}", aggregatePath, clusterPath, alarmPath);
        await agw.DisposeAsync();
        await (cluWriter!).DisposeAsync();
        await alw.DisposeAsync();

        _logger.LogInformation("蜃ｺ蜉・髮・ｨ・{Agg} 繧ｯ繝ｩ繧ｹ繧ｿ:{Clu} 繧｢繝ｩ繝ｼ繝:{Alm}", aggregatePath, clusterPath, alarmPath);
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
            if (removed > 0) _logger.LogDebug("蜿､縺・け繝ｩ繧ｹ繧ｿ遐ｴ譽・ {Count}", removed);
        }

        public void Dispose()
        {
            _grid.Clear();
        }
    }
    }
}

