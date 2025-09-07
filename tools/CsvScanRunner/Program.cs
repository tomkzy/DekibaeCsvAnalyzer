using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DekibaeCsvAnalyzer.Domain;
using DekibaeCsvAnalyzer.Models;
using DekibaeCsvAnalyzer.Services;
using Microsoft.Extensions.Logging;

internal class Program
{
    private static async Task<int> Main(string[] args)
    {
        try { Console.OutputEncoding = Encoding.UTF8; } catch { }
        var (root, ic, lot, date, codebookPath, trendCodes) = ParseArgs(args);
        using var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));
        var logger = loggerFactory.CreateLogger("Runner");

        if (string.IsNullOrWhiteSpace(root))
        {
            root = TryReadInputRootFromAppSettings() ?? Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "sandbox", "inputRoot"));
        }
        root = Path.GetFullPath(root);
        logger.LogInformation("InputRoot: {Root}", root);

        var scanner = new PathScanner(loggerFactory.CreateLogger<PathScanner>());
        var files = scanner.Enumerate(root, ic: ic, lotNo: lot, date: date).ToArray();
        if (files.Length == 0)
        {
            logger.LogWarning("No CSV files matched the given filters.");
            return 1;
        }
        logger.LogInformation("Found {Count} file(s):\n{List}", files.Length, string.Join(Environment.NewLine, files.Select(p => "  - " + p)));

        // Load and analyze all records
        var loader = new CsvLoader(loggerFactory.CreateLogger<CsvLoader>());
        var cts = new CancellationTokenSource();
        var conditions = new ConditionSet
        {
            IC = ic ?? string.Empty,
            LotNo = lot ?? string.Empty,
            From = date?.Date,
            To = date?.Date.AddDays(1).AddTicks(-1),
        };

        var outRoot = TryReadOutputRootFromAppSettings();
        if (string.IsNullOrWhiteSpace(outRoot))
        {
            outRoot = Path.Combine(Path.GetDirectoryName(root) ?? root, "..", "out");
        }
        outRoot = Path.GetFullPath(outRoot);
        logger.LogInformation("OutputRoot: {Root}", outRoot);
        var analyzer = new Analyzer(loggerFactory.CreateLogger<Analyzer>());
        var result = await analyzer.RunAsync(LoadAll(files, loader, cts.Token), conditions, outRoot, cts.Token);
        logger.LogInformation("Aggregate: {Agg}\nCluster: {Clu}\nAlarm: {Alm}", result.AggregatePath, result.ClusterPath, result.AlarmPath);

        // Optional: Daily trend for selected codes
        var trendList = NormalizeTrendCodes(ParseTrendCodes(trendCodes, loggerFactory.CreateLogger("Codes")), codebookPath, loggerFactory.CreateLogger("Codes"));
        if (trendList.Count > 0)
        {
            var trend = new TrendAggregator(loggerFactory.CreateLogger<TrendAggregator>());
            // 再読み込みでトレンド集計（シンプルに二度読み）
            var trendPath = await trend.WriteDailyTrendAsync(LoadAll(files, loader, cts.Token), conditions, trendList, outRoot, cts.Token);
            logger.LogInformation("Trend: {Trend}", trendPath);
        }

        return 0;
    }

    private static IAsyncEnumerable<InspectionRecord> LoadAll(IEnumerable<string> files, CsvLoader loader, CancellationToken ct)
    {
        return MergeAsync(files.Select(f => loader.LoadAsync(f, ct)));
    }

    private static async IAsyncEnumerable<T> MergeAsync<T>(IEnumerable<IAsyncEnumerable<T>> sources, [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var src in sources)
        {
            await foreach (var item in src.WithCancellation(ct))
            {
                yield return item;
            }
        }
    }

    private static (string? root, string? ic, string? lot, DateTime? date, string? codebook, string? trendCodes) ParseArgs(string[] args)
    {
        string? root = null; string? ic = null; string? lot = null; DateTime? date = null; string? codebook = null; string? trendCodes = null;
        foreach (var a in args)
        {
            var (k, v) = SplitKeyValue(a);
            if (k == null) continue;
            switch (k)
            {
                case "--root": root = v; break;
                case "--ic": ic = v; break;
                case "--lot": lot = v; break;
                case "--date":
                    if (DateTime.TryParseExact(v, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d)) date = d;
                    break;
                case "--codebook": codebook = v; break;
                case "--trend-codes": trendCodes = v; break;
            }
        }
        if (string.IsNullOrWhiteSpace(codebook)) codebook = TryReadCodebookFromAppSettings();
        return (root, ic, lot, date, codebook, trendCodes);
    }

    private static (string? key, string? value) SplitKeyValue(string arg)
    {
        if (arg.StartsWith("--"))
        {
            var idx = arg.IndexOf('=');
            if (idx > 0) return (arg.Substring(0, idx), arg.Substring(idx + 1));
            return (arg, null);
        }
        return (null, null);
    }

    private static string? TryReadInputRootFromAppSettings()
    {
        try
        {
            // locate BizCsvAnalyzer/src/appsettings.json relative to this runner
            var baseDir = AppContext.BaseDirectory;
            var candidate = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", "..", "src", "appsettings.json"));
            if (!File.Exists(candidate)) return null;
            using var s = File.OpenRead(candidate);
            using var doc = JsonDocument.Parse(s);
            if (doc.RootElement.TryGetProperty("Paths", out var paths) && paths.TryGetProperty("InputRoot", out var inputRoot))
            {
                var v = inputRoot.GetString();
                if (!string.IsNullOrWhiteSpace(v))
                {
                    // Resolve relative to repo root if needed
                    return ResolvePathRelativeToRepo(v);
                }
            }
        }
        catch { }
        return null;
    }

    private static string? TryReadOutputRootFromAppSettings()
    {
        try
        {
            var baseDir = AppContext.BaseDirectory;
            var candidate = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", "..", "src", "appsettings.json"));
            if (!File.Exists(candidate)) return null;
            using var s = File.OpenRead(candidate);
            using var doc = JsonDocument.Parse(s);
            if (doc.RootElement.TryGetProperty("Paths", out var paths) && paths.TryGetProperty("OutputRoot", out var outputRoot))
            {
                var v = outputRoot.GetString();
                if (!string.IsNullOrWhiteSpace(v))
                {
                    return ResolvePathRelativeToRepo(v);
                }
            }
        }
        catch { }
        return null;
    }

    private static string? TryReadCodebookFromAppSettings()
    {
        try
        {
            var baseDir = AppContext.BaseDirectory;
            var candidate = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", "..", "src", "appsettings.json"));
            if (!File.Exists(candidate)) return null;
            using var s = File.OpenRead(candidate);
            using var doc = JsonDocument.Parse(s);
            if (doc.RootElement.TryGetProperty("Paths", out var paths) && paths.TryGetProperty("CodebookPath", out var codebook))
            {
                var v = codebook.GetString();
                if (!string.IsNullOrWhiteSpace(v)) return v;
            }
        }
        catch { }
        return null;
    }

    private static List<string> ParseTrendCodes(string? trendCodes, Microsoft.Extensions.Logging.ILogger logger)
    {
        if (string.IsNullOrWhiteSpace(trendCodes)) return new List<string>();
        var arr = trendCodes.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Trim())
            .Where(x => x.Length > 0)
            .ToList();
        logger.LogInformation("Trend対象コード: {Codes}", string.Join(", ", arr));
        return arr;
    }

    private static List<string> NormalizeTrendCodes(List<string> list, string? codebookPath, Microsoft.Extensions.Logging.ILogger logger)
    {
        if (list.Count == 0) return list;
        // 可能ならコードブックを使って 'Kizu' → '01_Kizu' 等へ正規化
        try
        {
            if (!string.IsNullOrWhiteSpace(codebookPath))
            {
                var resolved = ResolvePathRelativeToRepo(codebookPath);
                var repo = new DekibaeCsvAnalyzer.Services.DefectCodeRepository(new Microsoft.Extensions.Logging.LoggerFactory().CreateLogger<DekibaeCsvAnalyzer.Services.DefectCodeRepository>(), resolved);
                var outList = new List<string>();
                foreach (var s in list)
                {
                    if (s.Contains('_')) { outList.Add(s); continue; }
                    // 数字2桁 → コード
                    if (int.TryParse(s, out var num)) { outList.Add(num.ToString("00") + "_" + s); continue; }
                    // Key から検索
                    DekibaeCsvAnalyzer.Models.DefectCode? d;
                    if (repo.TryGetByKey(s, out d) && d != null) { outList.Add(d.ToString()); }
                    else { outList.Add(s); }
                }
                logger.LogInformation("正規化済みコード: {Codes}", string.Join(", ", outList));
                return outList;
            }
        }
        catch { }
        return list;
    }

    private static string ResolvePathRelativeToRepo(string path)
    {
        if (Path.IsPathRooted(path)) return path;
        // tools/CsvScanRunner/bin/Debug/net8.0/ → DekibaeCsvAnalyzer
        var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        return Path.GetFullPath(Path.Combine(repoRoot, path));
    }
}
