using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;

/*
  例外/ロギング/キャンセル方針
  - Domainルールの検証クラス。例外は使わず INotifyDataErrorInfo によるバリデーションで表現。
  - ロギング/キャンセルは行わない（UI/アプリ層で必要なら監視）。
*/

namespace BizCsvAnalyzer.Domain
{
    public sealed class ConditionSet : INotifyDataErrorInfo
    {
        public string IC { get; set; }
        public string LotNo { get; set; }
        public string EquipmentCode { get; set; }
        public string CodeFilter { get; set; } // 正規化済KeyやCodeRaw（部分一致可）
        public int? SeverityMin { get; set; }
        public DateTime? From { get; set; }
        public DateTime? To { get; set; }

        public double ClusterRadius { get; set; } = 3.0; // r
        public TimeSpan ClusterTimeWindow { get; set; } = TimeSpan.FromSeconds(60); // t
        public TimeSpan AlarmWindow { get; set; } = TimeSpan.FromSeconds(300); // Δt
        public int AlarmThreshold { get; set; } = 10;

        private readonly Dictionary<string, List<string>> _errors = new Dictionary<string, List<string>>();
        public bool HasErrors { get { return _errors.Count > 0; } }
        public event EventHandler<DataErrorsChangedEventArgs> ErrorsChanged;

        public void Validate()
        {
            _errors.Clear();
            if (From.HasValue && To.HasValue && From > To)
            {
                AddError(nameof(From), "From は To 以下である必要があります。");
                AddError(nameof(To), "To は From 以上である必要があります。");
            }
            if (ClusterRadius <= 0) AddError(nameof(ClusterRadius), "ClusterRadius は正の数である必要があります。");
            if (ClusterTimeWindow <= TimeSpan.Zero) AddError(nameof(ClusterTimeWindow), "ClusterTimeWindow は正の期間である必要があります。");
            if (AlarmWindow <= TimeSpan.Zero) AddError(nameof(AlarmWindow), "AlarmWindow は正の期間である必要があります。");
            if (AlarmThreshold < 0) AddError(nameof(AlarmThreshold), "AlarmThreshold は 0 以上である必要があります。");
            RaiseAll();
        }

        private void AddError(string property, string message)
        {
            List<string> list;
            if (!_errors.TryGetValue(property, out list)) _errors[property] = list = new List<string>();
            list.Add(message);
        }
        private void RaiseAll()
        {
            if (ErrorsChanged == null) return;
            foreach (var key in _errors.Keys)
                ErrorsChanged(this, new DataErrorsChangedEventArgs(key));
        }
        public IEnumerable GetErrors(string propertyName)
        {
            if (propertyName == null) return new string[0];
            List<string> list;
            return _errors.TryGetValue(propertyName, out list) ? list : new string[0];
        }
    }
}
