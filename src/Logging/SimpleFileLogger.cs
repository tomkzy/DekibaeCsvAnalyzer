using System;
using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;

/*
  例外/ロギング/キャンセル方針
  - 目的: out/logs/app_yyyy-MM-dd.log への簡易ローリング出力（業務要件の基礎満たす）
  - 例外: 書き込み失敗は飲み込み、Console にフォールバック（アプリ継続を優先）。
  - マルチスレッド: lockで単純同期。高スループット用途ではSerilog等への置換を推奨。
  - キャンセル: ロガー自体は非同期/キャンセル未対応。上位の処理キャンセルに影響しない。
*/

namespace DekibaeCsvAnalyzer.Logging
{
    public static class Logging
    {
        public static (ILoggerFactory Factory, string LogPath) SimpleFileLoggerFactory(string logsRoot)
        {
            Directory.CreateDirectory(logsRoot);
            var provider = new FileLoggerProvider(logsRoot);
            var factory = LoggerFactory.Create(b =>
            {
                b.AddConsole();
                b.AddProvider(provider);
            });
            var today = DateTime.UtcNow.ToLocalTime().ToString("yyyy-MM-dd");
            return (factory, Path.Combine(logsRoot, $"app_{today}.log"));
        }
    }

    internal sealed class FileLoggerProvider : ILoggerProvider
    {
        private readonly string _logsRoot;
        public FileLoggerProvider(string logsRoot) => _logsRoot = logsRoot;
        public ILogger CreateLogger(string categoryName) => new FileLogger(_logsRoot, categoryName);
        public void Dispose() { }
    }

    internal sealed class FileLogger : ILogger
    {
        private readonly string _logsRoot;
        private readonly string _category;
        private readonly object _lock = new object();
        private string _currentPath = string.Empty;

        public FileLogger(string logsRoot, string category)
        {
            _logsRoot = logsRoot;
            _category = category;
        }

        private sealed class NullScope : IDisposable { public static readonly NullScope Instance = new NullScope(); public void Dispose() { } }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            try
            {
                var now = DateTime.UtcNow.ToLocalTime();
                var path = Path.Combine(_logsRoot, string.Format("app_{0:yyyy-MM-dd}.log", now));
                var line = string.Format("{0:yyyy-MM-dd HH:mm:ss.fff} [{1}] {2}: {3}", now, logLevel, _category, formatter(state, exception));
                if (exception != null) line += Environment.NewLine + exception;
                lock (_lock)
                {
                    if (_currentPath != path)
                    {
                        _currentPath = path;
                    }
                    File.AppendAllText(path, line + Environment.NewLine, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));
                }
            }
            catch
            {
                // フォールバック: 何もしない（Consoleは別Providerが出力）
            }
        }
    }
}
