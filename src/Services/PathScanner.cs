using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;

/*
  例外/ロギング/キャンセル方針
  - 入力ルートが存在しない場合: 例外にせず空列挙を返す（要件準拠）。
  - ディレクトリアクセス不可/列挙失敗: WARN ログしスキップ継続。
  - キャンセル: 列挙ループ中に CancellationToken を監視して即時中断。
*/

namespace DekibaeCsvAnalyzer.Services
{
    public sealed class PathScanner
    {
        private readonly ILogger<PathScanner> _logger;
        public PathScanner(ILogger<PathScanner> logger) { _logger = logger; }

        public IEnumerable<string> Enumerate(
            string inputRoot,
            string? ic = null,
            string? lotNo = null,
            DateTime? date = null,
            DateTime? dateFrom = null,
            DateTime? dateTo = null,
            System.Threading.CancellationToken cancellationToken = default(System.Threading.CancellationToken))
        {
            if (!Directory.Exists(inputRoot)) yield break;

            IEnumerable<string> IcDirs()
            {
                if (!string.IsNullOrWhiteSpace(ic))
                {
                    var p = Path.Combine(inputRoot, ic);
                    if (Directory.Exists(p)) yield return p;
                    yield break;
                }
                foreach (var d in SafeEnumerateDirectories(inputRoot)) yield return d;
            }

            foreach (var icDir in IcDirs())
            {
                cancellationToken.ThrowIfCancellationRequested();

                IEnumerable<string> DateDirs()
                {
                    if (date.HasValue)
                    {
                        var p = Path.Combine(icDir, date.Value.ToString("yyyyMMdd"));
                        if (Directory.Exists(p)) yield return p;
                        yield break;
                    }
                    var from = dateFrom ?? DateTime.MinValue;
                    var to = dateTo ?? DateTime.MaxValue;
                    foreach (var d in SafeEnumerateDirectories(icDir))
                    {
                        var name = Path.GetFileName(d);
                        DateTime dt;
                        if (DateTime.TryParseExact(name, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out dt))
                        {
                            if (dt >= from.Date && dt <= to.Date) yield return d;
                        }
                    }
                }

                foreach (var dateDir in DateDirs())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    IEnumerable<string> LotDirs()
                    {
                        if (!string.IsNullOrWhiteSpace(lotNo))
                        {
                            var p = Path.Combine(dateDir, lotNo);
                            if (Directory.Exists(p)) yield return p;
                            yield break;
                        }
                        foreach (var d in SafeEnumerateDirectories(dateDir)) yield return d;
                    }

                    foreach (var lotDir in LotDirs())
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        foreach (var file in SafeEnumerateFiles(lotDir, "*.csv"))
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            yield return file;
                        }
                    }
                }
            }
        }

        private IEnumerable<string> SafeEnumerateDirectories(string path)
        {
            try
            {
                return Directory.EnumerateDirectories(path);
            }
            catch (Exception ex)
            {
                if (ex is UnauthorizedAccessException || ex is IOException)
                {
                    _logger.LogWarning(ex, "ディレクトリ列挙に失敗: {Path}", path);
                    return new string[0];
                }
                throw;
            }
        }

        private IEnumerable<string> SafeEnumerateFiles(string path, string pattern)
        {
            try
            {
                return Directory.EnumerateFiles(path, pattern, SearchOption.AllDirectories);
            }
            catch (Exception ex)
            {
                if (ex is UnauthorizedAccessException || ex is IOException)
                {
                    _logger.LogWarning(ex, "ファイル列挙に失敗: {Path}", path);
                    return new string[0];
                }
                throw;
            }
        }
    }
}
