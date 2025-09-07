using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using DekibaeCsvAnalyzer.Models;

namespace DekibaeCsvAnalyzer.Services
{
    public interface ICsvLoader
    {
        IAsyncEnumerable<InspectionRecord> LoadAsync(string path, CancellationToken ct = default(CancellationToken));
    }

    public sealed class CsvLoader : ICsvLoader
    {
        private readonly ILogger<CsvLoader> _logger;
        public CsvLoader(ILogger<CsvLoader> logger) { _logger = logger; }

        public async IAsyncEnumerable<InspectionRecord> LoadAsync(string path, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default(CancellationToken))
        {
            _logger.LogInformation("CSV読込開始: {Path}", path);
            if (!File.Exists(path)) yield break;

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, useAsync: true))
            using (var reader = new StreamReader(fs, DetectEncodingFromBOMOrDefault(fs, Encoding.UTF8), detectEncodingFromByteOrderMarks: true, bufferSize: 4096, leaveOpen: false))
            {
                // ベンダー(縦持ちメタ + 欠陥行)形式を先に検出
                if (await IsVendorFormatAsync(reader, ct))
                {
                    await foreach (var r in LoadVendorAsync(reader, ct))
                    {
                        yield return r;
                    }
                    yield break;
                }

                var det = await DetectDelimiterAndHeaderAsync(reader, ct);
                var delimiter = det.Delimiter ?? ",";
                var headerOffset = det.HeaderOffset;
                _logger.LogInformation("区切り文字: '{Delimiter}' ヘッダー行オフセット: {Offset}", delimiter.Replace("\t", "\\t"), headerOffset);

                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,
                    Delimiter = delimiter,
                    BadDataFound = null,
                    TrimOptions = TrimOptions.Trim,
                };

                // ヘッダーの余白や前置行への対応（Trim）
                config.PrepareHeaderForMatch = args => args.Header == null ? null : args.Header.Trim();

                // ゴミ行が存在する場合はヘッダー行までスキップ
                for (var i = 0; i < headerOffset; i++)
                {
                    await reader.ReadLineAsync();
                }

                using (var csv = new CsvReader(reader, config))
                {
                    csv.Context.RegisterClassMap<InspectionRecordMap>();

                    // ヘッダー読み
                    await csv.ReadAsync();
                    csv.ReadHeader();

                    var skipped = 0;
                    while (await csv.ReadAsync())
                    {
                        ct.ThrowIfCancellationRequested();
                        InspectionRecord rec = null;
                        var ok = false;
                        try
                        {
                            rec = csv.GetRecord<InspectionRecord>();
                            ok = true;
                        }
                        catch (Exception ex)
                        {
                            skipped++;
                            _logger.LogWarning(ex, "行スキップ: {Row}", csv.Context.Parser.Row);
                        }
                        if (ok && rec != null) yield return rec;
                    }
                    if (skipped > 0) _logger.LogWarning("スキップ行数: {Count}", skipped);
                }
            }
            _logger.LogInformation("CSV読込終了: {Path}", path);
        }

        private static Encoding DetectEncodingFromBOMOrDefault(Stream stream, Encoding @default)
        {
            if (!stream.CanSeek) return @default;
            var pos = stream.Position;
            try
            {
                var bom = new byte[4];
                stream.Read(bom, 0, 4);
                stream.Position = pos;
                if (bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF) return new UTF8Encoding(true);
            }
            finally { stream.Position = pos; }
            return @default;
        }

        private static async Task<(string Delimiter, int HeaderOffset)> DetectDelimiterAndHeaderAsync(StreamReader reader, CancellationToken ct)
        {
            if (!reader.BaseStream.CanSeek) return (",", 0);
            var pos = reader.BaseStream.Position;
            try
            {
                var lines = new List<string>();
                for (int i = 0; i < 50; i++)
                {
                    var l = await reader.ReadLineAsync();
                    if (l == null) break;
                    lines.Add(l);
                }

                var candidates = new[] { ",", "\t", ";", "|" };
                (string delim, int off, int score) best = (",", 0, -1);

                foreach (var d in candidates)
                {
                    for (int i = 0; i < lines.Count; i++)
                    {
                        var tokens = lines[i].Split(new[] { d }, StringSplitOptions.None)
                            .Select(t => (t ?? string.Empty).Trim().Trim('"').ToLowerInvariant())
                            .ToList();
                        var score = HeaderScore(tokens);
                        if (score > best.score)
                        {
                            best = (d, i, score);
                        }
                    }
                }

                if (best.score >= 4)
                {
                    reader.DiscardBufferedData();
                    reader.BaseStream.Seek(pos, SeekOrigin.Begin);
                    return (best.delim, best.off);
                }

                var fallbackLine = lines.FirstOrDefault();
                if (fallbackLine == null) return (",", 0);
                var fb = candidates
                    .Select(d => new { d, count = fallbackLine.Split(new[] { d }, StringSplitOptions.None).Length - 1 })
                    .OrderByDescending(x => x.count)
                    .First();
                reader.DiscardBufferedData();
                reader.BaseStream.Seek(pos, SeekOrigin.Begin);
                return (fb.count > 0 ? fb.d : ",", 0);
            }
            finally
            {
                reader.DiscardBufferedData();
                reader.BaseStream.Seek(pos, SeekOrigin.Begin);
            }
        }

        private static int HeaderScore(IReadOnlyCollection<string> tokens)
        {
            if (tokens.Count == 0) return 0;
            bool Has(params string[] names) => names.Any(n => tokens.Contains(n));
            int s = 0;
            if (Has("lotno", "lot", "lot_no", "lot number")) s++;
            if (Has("timestamp", "time", "datetime", "ymd-hms")) s++;
            if (Has("equipmentcode", "ic", "eq", "equipment")) s++;
            if (Has("ledgerno", "ledger", "ld")) s++;
            if (Has("x", "posx")) s++;
            if (Has("y", "posy")) s++;
            if (Has("severity", "sev", "rank")) s++;
            if (Has("coderaw", "code", "defect", "ngcode")) s++;
            return s;
        }

        private static async Task<bool> IsVendorFormatAsync(StreamReader reader, CancellationToken ct)
        {
            if (!reader.BaseStream.CanSeek) return false;
            var pos = reader.BaseStream.Position;
            try
            {
                // 先頭行を見て 'IC' 単語かどうか
                var first = await reader.ReadLineAsync();
                if (first == null) return false;
                var head = first.Split(',')[0].Trim();
                return string.Equals(head, "IC", StringComparison.OrdinalIgnoreCase);
            }
            finally
            {
                reader.DiscardBufferedData();
                reader.BaseStream.Seek(pos, SeekOrigin.Begin);
            }
        }

        private async IAsyncEnumerable<InspectionRecord> LoadVendorAsync(StreamReader reader, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            // フォーマット:
            // 1: IC
            // 2: LotNo(8桁)
            // 3: SheetNo(1-2桁)
            // 4: yyyyMMdd-HHmmss
            // 5: EquipmentCode (例: NG1ISL001)
            // 6: 台帳No (例: 9123-321)
            // 7: 補材No (例: H-2) — 未使用
            // 8: 空行
            // 9: 面情報 (FT/BK)
            // 10: 検出数
            // 11以降: 欠陥行: CodeRaw,X,Y,Area,R,G,B,Hue,Luminance,Saturation,circularity,convexity,rectangularity,Sobel値,LongSide,ShortSide,Phi,ピース間同一連続,シート間同一数
            // 最終行に PrevData,... が来る

            string ReadFirstField(string? line)
            {
                if (line == null) return string.Empty;
                var i = line.IndexOf(',');
                return (i >= 0 ? line.Substring(0, i) : line).Trim();
            }

            // 必須メタ情報の取得
            var line1 = await reader.ReadLineAsync(); // IC
            var lotLine = await reader.ReadLineAsync();
            var sheetLine = await reader.ReadLineAsync();
            var dtLine = await reader.ReadLineAsync();
            var eqLine = await reader.ReadLineAsync();
            var ledgerLine = await reader.ReadLineAsync();
            var subLine = await reader.ReadLineAsync();
            var blankLine = await reader.ReadLineAsync();
            var faceLine = await reader.ReadLineAsync();
            var detCountLine = await reader.ReadLineAsync();

            string lot = ReadFirstField(lotLine);
            string equipment = ReadFirstField(eqLine);
            string ledger = ReadFirstField(ledgerLine);
            string dtToken = ReadFirstField(dtLine);
            string faceToken = ReadFirstField(faceLine); // "[FT]" or "[BK]"
            DateTime ts = default;
            // yyyyMMdd-HHmmss を優先、だめなら TryParse
            if (!DateTime.TryParseExact(dtToken, "yyyyMMdd-HHmmss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out ts))
            {
                DateTime.TryParse(dtToken, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out ts);
            }

            // 欠陥行を読み出す
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var line = await reader.ReadLineAsync();
                if (line == null) yield break;
                if (line.Length == 0) continue;
                var first = ReadFirstField(line);
                if (string.Equals(first, "PrevData", StringComparison.OrdinalIgnoreCase))
                {
                    // PrevData 行で終端
                    yield break;
                }

                var parts = line.Split(',');
                if (parts.Length < 3) continue;
                var code = (parts[0] ?? string.Empty).Trim();
                // 数値パース
                double px = 0, py = 0;
                if (!double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out px)) px = 0;
                if (!double.TryParse(parts[2], NumberStyles.Float, CultureInfo.InvariantCulture, out py)) py = 0;
                // 以降の指標を個別に取得（2~17列目をすべて保持）
                double area = GetD(parts, 3);
                int r = (int)Math.Round(GetD(parts, 4));
                int g = (int)Math.Round(GetD(parts, 5));
                int b = (int)Math.Round(GetD(parts, 6));
                double hue = GetD(parts, 7);
                double lum = GetD(parts, 8);
                double sat = GetD(parts, 9);
                double circ = GetD(parts, 10);
                double conv = GetD(parts, 11);
                double rect = GetD(parts, 12);
                double sobel = GetD(parts, 13);
                double longSide = GetD(parts, 14);
                double shortSide = GetD(parts, 15);
                double phi = GetD(parts, 16);
                int pieceRepeat = (int)Math.Round(GetD(parts, 17));
                int sheetRepeat = (int)Math.Round(GetD(parts, 18));

                yield return new InspectionRecord
                {
                    LotNo = lot ?? string.Empty,
                    Timestamp = ts,
                    EquipmentCode = equipment ?? string.Empty,
                    LedgerNo = ledger ?? string.Empty,
                    Face = faceToken ?? string.Empty,
                    X = px,
                    Y = py,
                    Area = area,
                    R = r,
                    G = g,
                    B = b,
                    Hue = hue,
                    Luminance = lum,
                    Saturation = sat,
                    Circularity = circ,
                    Convexity = conv,
                    Rectangularity = rect,
                    Sobel = sobel,
                    LongSide = longSide,
                    ShortSide = shortSide,
                    Phi = phi,
                    PieceRepeat = pieceRepeat,
                    SheetRepeat = sheetRepeat,
                    Severity = (int)Math.Round(sobel),
                    CodeRaw = code,
                };
            }
        }

        private static double GetD(string[] parts, int idx)
        {
            if (idx < 0 || idx >= parts.Length) return 0;
            double v; return double.TryParse(parts[idx], NumberStyles.Float, CultureInfo.InvariantCulture, out v) ? v : 0;
        }
    }
}
