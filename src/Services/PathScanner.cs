using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;

/*
  萓句､・繝ｭ繧ｮ繝ｳ繧ｰ/繧ｭ繝｣繝ｳ繧ｻ繝ｫ譁ｹ驥・
  - 蜈･蜉帙Ν繝ｼ繝医′蟄伜惠縺励↑縺・ｴ蜷・ 萓句､悶↓縺帙★遨ｺ蛻玲嫌繧定ｿ斐☆・郁ｦ∽ｻｶ貅匁侠・峨・
  - 繝・ぅ繝ｬ繧ｯ繝医Μ繧｢繧ｯ繧ｻ繧ｹ荳榊庄/蛻玲嫌螟ｱ謨・ WARN 繝ｭ繧ｰ縺励せ繧ｭ繝・・邯咏ｶ壹・
  - 繧ｭ繝｣繝ｳ繧ｻ繝ｫ: 蛻玲嫌繝ｫ繝ｼ繝嶺ｸｭ縺ｫ CancellationToken 繧堤屮隕悶＠縺ｦ蜊ｳ譎ゆｸｭ譁ｭ縲・
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
            System.Collections.Generic.IEnumerator<string>? e = null;
            try
            {
                try
                {
                    e = Directory.EnumerateDirectories(path).GetEnumerator();
                }
                catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "繝・ぅ繝ｬ繧ｯ繝医Μ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", path); yield break; }
                catch (IOException ex) { _logger.LogWarning(ex, "繝・ぅ繝ｬ繧ｯ繝医Μ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", path); yield break; }
                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    bool moved;
                    try { moved = e!.MoveNext(); }
                    catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "繝・ぅ繝ｬ繧ｯ繝医Μ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", path); yield break; }
                    catch (IOException ex) { _logger.LogWarning(ex, "繝・ぅ繝ｬ繧ｯ繝医Μ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", path); yield break; }
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

                System.Collections.Generic.IEnumerator<string>? fe = null;
                try
                {
                    try
                    {
                        fe = Directory.EnumerateFiles(dir, pattern, SearchOption.TopDirectoryOnly).GetEnumerator();
                    }
                    catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "繝輔ぃ繧､繝ｫ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", dir); goto PushDirs; }
                    catch (IOException ex) { _logger.LogWarning(ex, "繝輔ぃ繧､繝ｫ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", dir); goto PushDirs; }
                    while (true)
                    {
                        ct.ThrowIfCancellationRequested();
                        bool moved;
                        try { moved = fe!.MoveNext(); }
                        catch (UnauthorizedAccessException ex) { _logger.LogWarning(ex, "繝輔ぃ繧､繝ｫ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", dir); break; }
                        catch (IOException ex) { _logger.LogWarning(ex, "繝輔ぃ繧､繝ｫ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", dir); break; }
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
                    _logger.LogWarning(ex, "繝・ぅ繝ｬ繧ｯ繝医Μ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", path);
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
                    _logger.LogWarning(ex, "繝輔ぃ繧､繝ｫ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", path);
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
                    _logger.LogWarning(ex, "繝・ぅ繝ｬ繧ｯ繝医Μ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", path);
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
                    _logger.LogWarning(ex, "繝輔ぃ繧､繝ｫ蛻玲嫌縺ｫ螟ｱ謨・ {Path}", path);
                    return new string[0];
                }
                throw;
            }
        }
    }
}

