using System;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;
using System.Globalization;

/*
  例外/ロギング/キャンセル方針
  - モデル。例外なし。
  - CSVヘッダ差異は ClassMap(InspectionRecordMap) 側で対応。
  - 変換失敗は CsvLoader 側でWARNログし、その行のみスキップ。
*/

namespace BizCsvAnalyzer.Models
{
    public sealed class InspectionRecord
    {
        public string LotNo { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public string EquipmentCode { get; set; } = string.Empty;
        public string LedgerNo { get; set; } = string.Empty;
        public double X { get; set; }
        public double Y { get; set; }
        public int Severity { get; set; }
        public string CodeRaw { get; set; } = string.Empty;
    }

    public sealed class InspectionRecordMap : ClassMap<InspectionRecord>
    {
        public InspectionRecordMap()
        {
            // ヘッダ別名の例をカバー（必要に応じて追加）
            Map(m => m.LotNo).Name("LotNo", "Lot", "Lot_No", "Lot Number");
            Map(m => m.Timestamp).Name("Timestamp", "Time", "DateTime", "YMD-HMS").TypeConverter(new YmdHmsConverter());
            Map(m => m.EquipmentCode).Name("EquipmentCode", "IC", "Eq", "Equipment");
            Map(m => m.LedgerNo).Name("LedgerNo", "Ledger", "LD");
            Map(m => m.X).Name("X", "PosX");
            Map(m => m.Y).Name("Y", "PosY");
            Map(m => m.Severity).Name("Severity", "Sev", "Rank");
            Map(m => m.CodeRaw).Name("CodeRaw", "Code", "Defect", "NgCode");
        }
    }

    internal sealed class YmdHmsConverter : DefaultTypeConverter
    {
        // 期待形式: "yyyyMMdd-HHmmss"
        public override object ConvertFromString(string text, IReaderRow row, MemberMapData memberMapData)
        {
            if (string.IsNullOrWhiteSpace(text)) return default(DateTime);
            var s = text.Trim();
            DateTime dt;
            if (DateTime.TryParseExact(s, "yyyyMMdd-HHmmss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt))
                return dt;
            // フォールバック: ISO/一般的な書式
            if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt))
                return dt;
            throw new TypeConverterException(this, memberMapData, text, row.Context, string.Format("Invalid timestamp: '{0}'", text));
        }
    }
}
