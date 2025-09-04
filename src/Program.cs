using System;
using System.IO;
using Microsoft.Extensions.Logging;
using BizCsvAnalyzer.Logging;

/*
  例外/ロギング/キャンセル方針
  - 本エントリは最小限の起動コード。例外は未処理で落とし呼び出し元/テストで検知。
  - ロギング: Console + out/logs/ への日毎ロールファイル（簡易実装）に出力。
  - キャンセル: 本体サービス（CsvLoader/Analyzer）に CancellationToken を伝播。
*/

namespace BizCsvAnalyzer
{
    internal static class Program
    {
        public static void Main(string[] args)
        {
            var pair = BizCsvAnalyzer.Logging.Logging.SimpleFileLoggerFactory(Path.Combine("out", "logs"));
            var loggerFactory = pair.Factory;
            var logPath = pair.LogPath;
            var logger = loggerFactory.CreateLogger("Bootstrap");
            logger.LogInformation("BizCsvAnalyzer booted. Logs at {Path}", logPath);
            // 本サンプルは CLI 実行機能を持たず、ライブラリ/テストで利用します。
            // UI( Avalonia )やCLIを追加する際は、Services.* をDIで組み合わせてください。
        }
    }
}
