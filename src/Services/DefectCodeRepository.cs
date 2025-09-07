using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Microsoft.Extensions.Logging;
using DekibaeCsvAnalyzer.Models;

/*
  萓句､・繝ｭ繧ｮ繝ｳ繧ｰ/繧ｭ繝｣繝ｳ繧ｻ繝ｫ譁ｹ驥・
  - 隱ｭ縺ｿ霎ｼ縺ｿ譎・ 荳肴ｭ｣陦後・繧ｹ繧ｭ繝・・縺励※ WARN 繝ｭ繧ｰ縲り・蜻ｽ逧ИO萓句､悶・荳贋ｽ阪∈蜀阪せ繝ｭ繝ｼ縲・
  - 繝ｭ繝ｼ繝峨・驕・ｻｶ蛻晄悄蛹悶＠縲√せ繝ｬ繝・ラ繧ｻ繝ｼ繝輔↓繧ｭ繝｣繝・す繝･縲ゅく繝｣繝ｳ繧ｻ繝ｫ縺ｯ譛ｪ蟇ｾ蠢懶ｼ亥ｰ剰ｦ乗ｨ｡I/O諠ｳ螳夲ｼ峨・
*/

namespace DekibaeCsvAnalyzer.Services
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
                _logger.LogWarning("繧ｳ繝ｼ繝峨ヶ繝・け縺瑚ｦ九▽縺九ｊ縺ｾ縺帙ｓ: {Path}", _codebookPath);
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
                        _logger.LogWarning("荳肴ｭ｣縺ｪ繧ｳ繝ｼ繝峨ヶ繝・け陦・{Line}): {Value}", lineNo, s);
                        continue;
                    }
                    var codeStr = s.Substring(0, uscore);
                    var key = s.Substring(uscore + 1);
                    int code;
                    if (!int.TryParse(codeStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out code))
                    {
                        _logger.LogWarning("荳肴ｭ｣縺ｪ繧ｳ繝ｼ繝臥分蜿ｷ({Line}): {Value}", lineNo, s);
                        continue;
                    }
                    var model = new DefectCode(code, key);
                    _byCode[code] = model;
                    _byKey[key] = model;
                }
            }
            _logger.LogInformation("繧ｳ繝ｼ繝峨ヶ繝・け隱ｭ霎ｼ: {Count}莉ｶ", _byCode.Count);
        }

        public bool TryGetByCode(int code, out DefectCode? defect)
        {
            EnsureLoaded();
            return _byCode.TryGetValue(code, out defect);
        }

        public bool TryGetByKey(string key, out DefectCode? defect)
        {
            EnsureLoaded();
            return _byKey.TryGetValue(key, out defect);
        }
    }
}

