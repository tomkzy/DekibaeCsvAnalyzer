using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;

/*
  ロギング/キャンセル方針
  - 入力ルートが存在しない場合は、例外にせず空列挙を返す（要件準拠）。
  - ディレクトリアクセス不可/列挙失敗は WARN ログしてスキップし継続。
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
            System.Threading.CancellationToken cancellationToken = default)
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
                foreach (var d in EnumerateDirectoriesTopSafe(inputRoot, cancellationToken)) yield return d;
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
                    var from = (dateFrom ?? DateTime.MinValue).Date;
                    var to = (dateTo ?? DateTime.MaxValue).Date;
                    foreach (var d in EnumerateDirectoriesTopSafe(icDir, cancellationToken))
                    {
                        var name = Path.GetFileName(d);
                        if (DateTime.TryParseExact(name, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out var dt))
                        {
                            if (dt.Date >= from && dt.Date <= to) yield return d;
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
                        foreach (var d in EnumerateDirectoriesTopSafe(dateDir, cancellationToken)) yield return d;
                    }

                    foreach (var lotDir in LotDirs())
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        foreach (var file in EnumerateFilesRecursiveSafe(lotDir, "*.csv", cancellationToken))
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            yield return file;
                        }
                    }
                }
            }
        }

        // Top directory only, 逐次例外を握りつつ列挙
        private IEnumerable<string> EnumerateDirectoriesTopSafe(string path, System.Threading.CancellationToken ct)
        {
            System.Collections.Generic.IEnumerator<string>? e = null;
            try
            {
                try
                {
                    e = Directory.EnumerateDirectories(path).GetEnumerator();
                }
                catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "ディレクトリ列挙に失敗 {Path}", path); yield break; }
                catch (IOException ex) { _logger.LogWarning(ex, "ディレクトリ列挙に失敗 {Path}", path); yield break; }
                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    bool moved;
                    try { moved = e!.MoveNext(); }
                    catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "ディレクトリ列挙に失敗 {Path}", path); yield break; }
                    catch (IOException ex) { _logger.LogWarning(ex, "ディレクトリ列挙に失敗 {Path}", path); yield break; }
                    if (!moved) yield break;
                    yield return e.Current!;
                }
            }
            finally
            {
                (e as IDisposable)?.Dispose();
            }
        }

        // 再帰的ファイル列挙（スタック使用）、逐次例外を握る
        private IEnumerable<string> EnumerateFilesRecursiveSafe(string root, string pattern, System.Threading.CancellationToken ct)
        {
            var stack = new Stack<string>();
            stack.Push(root);
            while (stack.Count > 0)
            {
                ct.ThrowIfCancellationRequested();
                var dir = stack.Pop();

                System.Collections.Generic.IEnumerator<string>? fe = null;
                try
                {
                    try
                    {
                        fe = Directory.EnumerateFiles(dir, pattern, SearchOption.TopDirectoryOnly).GetEnumerator();
                    }
                    catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "ファイル列挙に失敗 {Path}", dir); goto PushDirs; }
                    catch (IOException ex) { _logger.LogWarning(ex, "ファイル列挙に失敗 {Path}", dir); goto PushDirs; }
                    while (true)
                    {
                        ct.ThrowIfCancellationRequested();
                        bool moved;
                        try { moved = fe!.MoveNext(); }
                        catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "ファイル列挙に失敗 {Path}", dir); break; }
                        catch (IOException ex) { _logger.LogWarning(ex, "ファイル列挙に失敗 {Path}", dir); break; }
                        if (!moved) break;
                        yield return fe.Current!;
                    }
                }
                finally
                {
                    (fe as IDisposable)?.Dispose();
                }
            PushDirs:
                foreach (var sub in EnumerateDirectoriesTopSafe(dir, ct))
                {
                    stack.Push(sub);
                }
            }
        }
    }
}

