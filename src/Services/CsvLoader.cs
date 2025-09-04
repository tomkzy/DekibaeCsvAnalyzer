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
using BizCsvAnalyzer.Models;

/*
  例外/ロギング/キャンセル方針
  - 例外: IO/致命的CSV設定は上位へ再スロー。各行の型変換失敗はWARNログしてスキップ。
  - ロギング: 読み込み開始/終了、区切り文字推定、スキップ件数、ファイル名をINFO/WARNで出力。
  - キャンセル: ReadAsyncループでCancellationTokenを監視し即時中断可能。
*/

namespace BizCsvAnalyzer.Services
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
                var delimiter = await DetectDelimiterAsync(reader, ct) ?? ",";
                _logger.LogInformation("区切り文字: '{Delimiter}'", delimiter.Replace("\t", "\\t"));

                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,
                    Delimiter = delimiter,
                    BadDataFound = null,
                    TrimOptions = TrimOptions.Trim,
                };

                // ヘッダ前処理（Trim）
                config.PrepareHeaderForMatch = args => args.Header == null ? null : args.Header.Trim();
                using (var csv = new CsvReader(reader, config))
                {
                    csv.Context.RegisterClassMap<InspectionRecordMap>();

                    // ヘッダ読込
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

        private static async Task<string> DetectDelimiterAsync(StreamReader reader, CancellationToken ct)
        {
            if (!reader.BaseStream.CanSeek) return ",";
            var pos = reader.BaseStream.Position;
            try
            {
                var line = await reader.ReadLineAsync();
                if (line == null) return ",";
                var candidates = new[] { ",", "\t", ";", "|" };
                var best = candidates
                    .Select(d => new { d, count = line.Split(new[] { d }, StringSplitOptions.None).Length - 1 })
                    .OrderByDescending(x => x.count)
                    .First();
                reader.BaseStream.Seek(pos, SeekOrigin.Begin);
                reader.BaseStream.Position = pos;
                return best.count > 0 ? best.d : ",";
            }
            finally
            {
                reader.BaseStream.Seek(pos, SeekOrigin.Begin);
            }
        }
    }
}
