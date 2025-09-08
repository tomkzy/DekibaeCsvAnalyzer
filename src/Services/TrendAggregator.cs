using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DekibaeCsvAnalyzer.Domain;
using DekibaeCsvAnalyzer.Models;
using Microsoft.Extensions.Logging;

namespace DekibaeCsvAnalyzer.Services
{
    public sealed class TrendAggregator
    {
        private readonly ILogger<TrendAggregator> _logger;
        public TrendAggregator(ILogger<TrendAggregator> logger) { _logger = logger; }

        public async Task<string> WriteDailyTrendAsync(
            IAsyncEnumerable<InspectionRecord> source,
            ConditionSet conditions,
            IReadOnlyList<string> codeSelectors,
            string outputRoot,
            CancellationToken ct = default)
        {
            var outputsDir = Path.Combine(outputRoot, "exports");
            Directory.CreateDirectory(outputsDir);

            var tsStamp = DateTime.Now.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
            string icName = string.IsNullOrWhiteSpace(conditions.IC) ? "IC" : conditions.IC;
            string lotName = string.IsNullOrWhiteSpace(conditions.LotNo) ? "ALL" : conditions.LotNo;
            string dateRange = RangeLabel(conditions.From, conditions.To);
            var outPath = Path.Combine(outputsDir, $"Trend_{icName}_{lotName}_{dateRange}_{tsStamp}.csv");

            // 正規化したターゲット列名（例: '01_Kizu' 等）。null/空は何もしない
            var targets = NormalizeSelectors(codeSelectors);
            if (targets.Count == 0)
            {
                _logger.LogWarning("トレンド対象の検査コードが指定されていません（--trend-codes）。スキップします。");
                return outPath;
            }

            // 日付 -> code -> count
            var daily = new SortedDictionary<DateTime, Dictionary<string, int>>();
            await foreach (var r in source.WithCancellation(ct))
            {
                if (!Match(r, conditions)) continue;
                var day = r.Timestamp.Date;
                if (!daily.TryGetValue(day, out var bucket)) { bucket = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase); daily[day] = bucket; }

                var raw = r.CodeRaw ?? string.Empty;
                // 正規化（'01_Kizu' または 'Kizu' → '01_Kizu' を優先）
                var normalized = NormalizeCodeRaw(raw);
                if (!targets.Contains(normalized))
                {
                    // 'Kizu' だけ指定や CodeRaw が 'Kizu' の場合の互換対応
                    var keyOnly = ExtractKey(raw);
                    if (keyOnly == null || !targets.Contains(keyOnly)) continue;
                    normalized = keyOnly;
                }

                int c; bucket[normalized] = bucket.TryGetValue(normalized, out c) ? c + 1 : 1;
            }

            await using var w = new StreamWriter(outPath, false, new UTF8Encoding(true));
            // ヘッダー: Date, code1, code2, ...
            await w.WriteLineAsync(string.Join(',', new[] { "Date" }.Concat(targets)));
            foreach (var kv in daily)
            {
                var date = kv.Key.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                var row = new List<string> { date };
                foreach (var t in targets)
                {
                    int c; row.Add(kv.Value.TryGetValue(t, out c) ? c.ToString(CultureInfo.InvariantCulture) : "0");
                }
                await w.WriteLineAsync(string.Join(',', row));
            }

            _logger.LogInformation("トレンド出力: {Path}", outPath);
            return outPath;
        }

        private static string RangeLabel(DateTime? from, DateTime? to)
        {
            if (!from.HasValue && !to.HasValue) return "ALL";
            var f = (from ?? to ?? DateTime.MinValue).ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            var t = (to ?? from ?? DateTime.MinValue).ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            return f + "-" + t;
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

        private static string NormalizeCodeRaw(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return string.Empty;
            raw = raw.Trim();
            // 既に '01_Kizu' 形式
            var us = raw.IndexOf('_');
            if (us > 0 && us < raw.Length - 1) return raw;
            // '01' 単独の指定などは救済しない。キー単独は ExtractKey に任せる。
            return raw;
        }

        private static string? ExtractKey(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return null;
            var us = raw.IndexOf('_');
            if (us > 0 && us < raw.Length - 1) return raw.Substring(us + 1);
            return raw; // もともとキーだけ（例: 'Kizu'）
        }

        private static List<string> NormalizeSelectors(IEnumerable<string> sels)
        {
            var list = new List<string>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var s in sels ?? Array.Empty<string>())
            {
                if (string.IsNullOrWhiteSpace(s)) continue;
                var v = s.Trim();
                if (seen.Add(v)) list.Add(v);
            }
            return list;
        }
    }
}
