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
                    var from = dateFrom ?? DateTime.MinValue;
                    var to = dateTo ?? DateTime.MaxValue;
                    foreach (var d in EnumerateDirectoriesTopSafe(icDir, cancellationToken))
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

        // Streaming-safe directory enumeration (top directory only), with per-iteration exception handling
        private IEnumerable<string> EnumerateDirectoriesTopSafe(string path, System.Threading.CancellationToken ct)
        {
            var e = Directory.EnumerateDirectories(path).GetEnumerator();
            try
            {
                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    bool moved;
                    try { moved = e.MoveNext(); }
                    catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "ディレクトリ列挙に失敗: {Path}", path); yield break; }
                    catch (IOException ex) { _logger.LogWarning(ex, "ディレクトリ列挙に失敗: {Path}", path); yield break; }
                    if (!moved) yield break;
                    yield return e.Current!;
                }
            }
            finally
            {
                (e as IDisposable)?.Dispose();
            }
        }

        // Streaming, stack-based recursive file enumeration with cancellation and per-iteration exception handling
        private IEnumerable<string> EnumerateFilesRecursiveSafe(string root, string pattern, System.Threading.CancellationToken ct)
        {
            var stack = new Stack<string>();
            stack.Push(root);
            while (stack.Count > 0)
            {
                ct.ThrowIfCancellationRequested();
                var dir = stack.Pop();

                var fe = Directory.EnumerateFiles(dir, pattern, SearchOption.TopDirectoryOnly).GetEnumerator();
                try
                {
                    while (true)
                    {
                        ct.ThrowIfCancellationRequested();
                        bool moved;
                        try { moved = fe.MoveNext(); }
                        catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "ファイル列挙に失敗: {Path}", dir); break; }
                        catch (IOException ex) { _logger.LogWarning(ex, "ファイル列挙に失敗: {Path}", dir); break; }
                        if (!moved) break;
                        yield return fe.Current!;
                    }
                }
                finally
                {
                    (fe as IDisposable)?.Dispose();
                }

                foreach (var sub in EnumerateDirectoriesTopSafe(dir, ct))
                {
                    stack.Push(sub);
                }
            }
        }

        // Eager enumeration wrappers that catch per-iteration exceptions to avoid bubbling from lazy Enumerate* APIs
        private IEnumerable<string> SafeEnumerateDirectoriesEager(string path)
        {
            try
            {
                // Eagerly materialize to capture any exceptions here
                return System.Linq.Enumerable.ToArray(Directory.EnumerateDirectories(path));
            }
            catch (Exception ex)
            {
                if (ex is UnauthorizedAccessException || ex is IOException)
                {
                    _logger.LogWarning(ex, "ディレクトリ列挙に失敗: {Path}", path);
                    return Array.Empty<string>();
                }
                throw;
            }
        }

        private IEnumerable<string> SafeEnumerateFilesEager(string path, string pattern)
        {
            try
            {
                // Eagerly materialize to capture any exceptions here
                return System.Linq.Enumerable.ToArray(Directory.EnumerateFiles(path, pattern, SearchOption.AllDirectories));
            }
            catch (Exception ex)
            {
                if (ex is UnauthorizedAccessException || ex is IOException)
                {
                    _logger.LogWarning(ex, "ファイル列挙に失敗: {Path}", path);
                    return Array.Empty<string>();
                }
                throw;
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
