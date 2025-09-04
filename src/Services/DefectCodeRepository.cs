using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Microsoft.Extensions.Logging;
using BizCsvAnalyzer.Models;

/*
  例外/ロギング/キャンセル方針
  - 読み込み時: 不正行はスキップして WARN ログ。致命的IO例外は上位へ再スロー。
  - ロードは遅延初期化し、スレッドセーフにキャッシュ。キャンセルは未対応（小規模I/O想定）。
*/

namespace BizCsvAnalyzer.Services
{
    public sealed class DefectCodeRepository
    {
        private readonly ILogger<DefectCodeRepository> _logger;
        private readonly string _codebookPath;
        private readonly ConcurrentDictionary<int, DefectCode> _byCode = new ConcurrentDictionary<int, DefectCode>();
        private readonly ConcurrentDictionary<string, DefectCode> _byKey = new ConcurrentDictionary<string, DefectCode>(StringComparer.OrdinalIgnoreCase);
        private volatile bool _loaded;

        public DefectCodeRepository(ILogger<DefectCodeRepository> logger, string codebookPath)
        {
            _logger = logger;
            _codebookPath = codebookPath;
        }

        private void EnsureLoaded()
        {
            if (_loaded) return;
            lock (_byCode)
            {
                if (_loaded) return;
                Load();
                _loaded = true;
            }
        }

        private void Load()
        {
            _byCode.Clear();
            _byKey.Clear();

            if (!File.Exists(_codebookPath))
            {
                _logger.LogWarning("コードブックが見つかりません: {Path}", _codebookPath);
                return;
            }

            using (var sr = new StreamReader(_codebookPath))
            {
                string line;
                var lineNo = 0;
                while ((line = sr.ReadLine()) != null)
                {
                    lineNo++;
                    var s = line.Trim();
                    if (s.Length == 0 || s.StartsWith("#")) continue;
                    var uscore = s.IndexOf('_');
                    if (uscore <= 0 || uscore == s.Length - 1)
                    {
                        _logger.LogWarning("不正なコードブック行({Line}): {Value}", lineNo, s);
                        continue;
                    }
                    var codeStr = s.Substring(0, uscore);
                    var key = s.Substring(uscore + 1);
                    int code;
                    if (!int.TryParse(codeStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out code))
                    {
                        _logger.LogWarning("不正なコード番号({Line}): {Value}", lineNo, s);
                        continue;
                    }
                    var model = new DefectCode(code, key);
                    _byCode[code] = model;
                    _byKey[key] = model;
                }
            }
            _logger.LogInformation("コードブック読込: {Count}件", _byCode.Count);
        }

        public bool TryGetByCode(int code, out DefectCode defect)
        {
            EnsureLoaded();
            return _byCode.TryGetValue(code, out defect);
        }

        public bool TryGetByKey(string key, out DefectCode defect)
        {
            EnsureLoaded();
            return _byKey.TryGetValue(key, out defect);
        }
    }
}
