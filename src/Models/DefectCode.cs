using System;

/*
  例外/ロギング/キャンセル方針
  - 単純モデル。例外発生しない設計（不正は Repository 側で検知/ログ）。
  - ロギング/キャンセルは使用しない。
*/

namespace BizCsvAnalyzer.Models
{
    public sealed class DefectCode
    {
        public int Code { get; }
        public string Key { get; }

        public DefectCode(int code, string key)
        {
            Code = code;
            Key = key;
        }

        public override string ToString() => string.Format("{0:D2}_{1}", Code, Key);
    }
}
