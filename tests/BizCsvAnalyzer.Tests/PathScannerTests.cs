using System;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using Xunit;
using BizCsvAnalyzer.Services;

namespace BizCsvAnalyzer.Tests
{
public class PathScannerTests
{
    [Fact]
    public void Enumerate_FindsCsv_ByPattern()
    {
        var root = Path.Combine(Path.GetTempPath(), "BizCsvAnalyzer_Test_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var ic = "NG1ISL001";
            var date = DateTime.Today;
            var lot = "24081234";
            var baseDir = Path.Combine(root, ic, date.ToString("yyyyMMdd"), lot, "sub");
            Directory.CreateDirectory(baseDir);
            File.WriteAllText(Path.Combine(baseDir, "a.csv"), "h1,h2\n1,2\n");
            File.WriteAllText(Path.Combine(baseDir, "b.txt"), "noop");

            using var lf = LoggerFactory.Create(b => { });
            var scanner = new PathScanner(lf.CreateLogger<PathScanner>());
            var files = scanner.Enumerate(root, ic: ic, lotNo: lot, date: date).ToArray();

            Assert.Single(files);
            Assert.EndsWith("a.csv", files[0]);
        }
        finally
        {
            try { Directory.Delete(root, true); } catch { }
        }
    }
}
}
