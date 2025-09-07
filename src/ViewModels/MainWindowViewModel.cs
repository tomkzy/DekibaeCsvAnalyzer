using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Input;
using DekibaeCsvAnalyzer.Domain;
using DekibaeCsvAnalyzer.Services;
using DekibaeCsvAnalyzer.Utils;
using Microsoft.Extensions.Logging;

namespace DekibaeCsvAnalyzer.ViewModels
{
    public sealed class MainWindowViewModel : INotifyPropertyChanged
    {
        private string _inputRoot = "sandbox/inputRoot";
        private string _outputRoot = "out";
        private string _codebookPath = "data/codebook/defect_codes.csv";

        private string _ic = string.Empty;
        private string _lotNo = string.Empty;
        private DateTimeOffset? _date;
        private bool _useDateRange;
        private DateTimeOffset? _dateFrom;
        private DateTimeOffset? _dateTo;

        private double _clusterRadius = 3.0;
        private int _clusterTimeWindowSec = 60;
        private int _alarmWindowSec = 300;
        private int _alarmThreshold = 10;

        private bool _isRunning;
        private string _statusText = string.Empty;
        private CancellationTokenSource? _cts;

        public event PropertyChangedEventHandler? PropertyChanged;

        public string InputRoot { get => _inputRoot; set { _inputRoot = value; OnPropertyChanged(); } }
        public string OutputRoot { get => _outputRoot; set { _outputRoot = value; OnPropertyChanged(); } }
        public string CodebookPath { get => _codebookPath; set { _codebookPath = value; OnPropertyChanged(); } }

        public string IC { get => _ic; set { _ic = value; OnPropertyChanged(); } }
        public string LotNo { get => _lotNo; set { _lotNo = value; OnPropertyChanged(); } }
        public DateTimeOffset? Date { get => _date; set { _date = value; OnPropertyChanged(); } }
        public bool UseDateRange { get => _useDateRange; set { _useDateRange = value; OnPropertyChanged(); } }
        public DateTimeOffset? DateFrom { get => _dateFrom; set { _dateFrom = value; OnPropertyChanged(); } }
        public DateTimeOffset? DateTo { get => _dateTo; set { _dateTo = value; OnPropertyChanged(); } }

        public double ClusterRadius { get => _clusterRadius; set { _clusterRadius = value; OnPropertyChanged(); } }
        public int ClusterTimeWindowSec { get => _clusterTimeWindowSec; set { _clusterTimeWindowSec = value; OnPropertyChanged(); } }
        public int AlarmWindowSec { get => _alarmWindowSec; set { _alarmWindowSec = value; OnPropertyChanged(); } }
        public int AlarmThreshold { get => _alarmThreshold; set { _alarmThreshold = value; OnPropertyChanged(); } }

        public bool IsRunning { get => _isRunning; private set { _isRunning = value; OnPropertyChanged(); OnPropertyChanged(nameof(IsNotRunning)); _runCommand?.RaiseCanExecuteChanged(); _cancelCommand?.RaiseCanExecuteChanged(); } }
        public bool IsNotRunning => !IsRunning;
        public string StatusText { get => _statusText; private set { _statusText = value; OnPropertyChanged(); } }

        public ObservableCollection<string> RecentOutputs { get; } = new ObservableCollection<string>();

        private RelayCommand? _runCommand;
        public ICommand RunCommand => _runCommand ??= new RelayCommand(async _ => await RunAsync(), _ => !IsRunning);

        private RelayCommand? _cancelCommand;
        public ICommand CancelCommand => _cancelCommand ??= new RelayCommand(_ => Cancel(), _ => IsRunning);

        private RelayCommand? _saveSettingsCommand;
        public ICommand SaveSettingsCommand => _saveSettingsCommand ??= new RelayCommand(_ => SaveSettings());

        private void OnPropertyChanged([CallerMemberName] string? name = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        public MainWindowViewModel()
        {
            TryLoadSettings();
        }

        private async Task RunAsync()
        {
            IsRunning = true;
            StatusText = "Analysis started...";
            _cts = new CancellationTokenSource();
            try
            {
                var ct = _cts.Token;

                // Logging factory (file + console)
                var (factory, logPath) = DekibaeCsvAnalyzer.Logging.Logging.SimpleFileLoggerFactory(Path.Combine(OutputRootResolved(), "logs"));
                StatusText = $"Log: {logPath}";

                var scanner = new PathScanner(factory.CreateLogger<PathScanner>());
                var loader = new CsvLoader(factory.CreateLogger<CsvLoader>());
                var analyzer = new Analyzer(factory.CreateLogger<Analyzer>());

                // Build conditions
                DateTime? from = null; DateTime? to = null;
                if (UseDateRange)
                {
                    if (DateFrom.HasValue) from = DateFrom.Value.Date;
                    if (DateTo.HasValue) to = DateTo.Value.Date.AddDays(1).AddTicks(-1);
                }
                else if (Date.HasValue)
                {
                    from = Date.Value.Date;
                    to = Date.Value.Date.AddDays(1).AddTicks(-1);
                }

                var cond = new ConditionSet
                {
                    IC = IC ?? string.Empty,
                    LotNo = LotNo ?? string.Empty,
                    From = from,
                    To = to,
                    ClusterRadius = ClusterRadius,
                    ClusterTimeWindow = TimeSpan.FromSeconds(Math.Max(1, ClusterTimeWindowSec)),
                    AlarmWindow = TimeSpan.FromSeconds(Math.Max(1, AlarmWindowSec)),
                    AlarmThreshold = Math.Max(0, AlarmThreshold),
                };

                var root = Path.GetFullPath(InputRoot);
                var files = scanner.Enumerate(root, ic: string.IsNullOrWhiteSpace(IC) ? null : IC, lotNo: string.IsNullOrWhiteSpace(LotNo) ? null : LotNo, date: UseDateRange ? null : Date?.DateTime, cancellationToken: ct);

                // Merge async enumerables (sequentially)
                async IAsyncEnumerable<Models.InspectionRecord> LoadAll([EnumeratorCancellation] CancellationToken token)
                {
                    foreach (var f in files)
                    {
                        await foreach (var r in loader.LoadAsync(f, token).WithCancellation(token))
                        {
                            yield return r;
                        }
                    }
                }

                var outRoot = OutputRootResolved();
                Directory.CreateDirectory(Path.Combine(outRoot, "exports"));
                var result = await analyzer.RunAsync(LoadAll(ct), cond, outRoot, ct);
                RecentOutputs.Insert(0, result.AggregatePath);
                RecentOutputs.Insert(0, result.ClusterPath);
                RecentOutputs.Insert(0, result.AlarmPath);
                StatusText = "Analysis completed.";
            }
            catch (OperationCanceledException)
            {
                StatusText = "Canceled.";
            }
            catch (Exception ex)
            {
                StatusText = "Error: " + ex.Message;
            }
            finally
            {
                _cts?.Dispose();
                _cts = null;
                IsRunning = false;
            }
        }

        private void Cancel()
        {
            _cts?.Cancel();
        }

        private string OutputRootResolved()
        {
            var p = OutputRoot;
            if (string.IsNullOrWhiteSpace(p)) p = "out";
            return Path.GetFullPath(p);
        }

        private void TryLoadSettings()
        {
            try
            {
                var appSettings = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "appsettings.json");
                var settingsPath = Path.GetFullPath(appSettings);
                if (!File.Exists(settingsPath)) return;
                using var s = File.OpenRead(settingsPath);
                using var doc = JsonDocument.Parse(s);
                if (doc.RootElement.TryGetProperty("Paths", out var paths))
                {
                    if (paths.TryGetProperty("InputRoot", out var ir) && !string.IsNullOrWhiteSpace(ir.GetString())) InputRoot = ir.GetString()!;
                    if (paths.TryGetProperty("OutputRoot", out var or) && !string.IsNullOrWhiteSpace(or.GetString())) OutputRoot = or.GetString()!;
                    if (paths.TryGetProperty("CodebookPath", out var cb) && !string.IsNullOrWhiteSpace(cb.GetString())) CodebookPath = cb.GetString()!;
                }
                if (doc.RootElement.TryGetProperty("Analyzer", out var an))
                {
                    if (an.TryGetProperty("ClusterRadius", out var cr)) ClusterRadius = cr.GetDouble();
                    if (an.TryGetProperty("ClusterTimeWindowSec", out var tw)) ClusterTimeWindowSec = tw.GetInt32();
                    if (an.TryGetProperty("AlarmWindowSec", out var aw)) AlarmWindowSec = aw.GetInt32();
                    if (an.TryGetProperty("AlarmThreshold", out var th)) AlarmThreshold = th.GetInt32();
                }
            }
            catch { }
        }

        private void SaveSettings()
        {
            try
            {
                var appSettings = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "appsettings.json");
                var settingsPath = Path.GetFullPath(appSettings);
                using var doc = JsonDocument.Parse(File.Exists(settingsPath) ? File.ReadAllText(settingsPath) : "{ }");
                using var ms = new MemoryStream();
                using var w = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = true });
                w.WriteStartObject();
                // Paths
                w.WritePropertyName("Paths");
                w.WriteStartObject();
                w.WriteString("InputRoot", InputRoot ?? string.Empty);
                w.WriteString("OutputRoot", OutputRoot ?? "out");
                w.WriteString("CodebookPath", CodebookPath ?? "data/codebook/defect_codes.csv");
                w.WriteEndObject();
                // Analyzer
                w.WritePropertyName("Analyzer");
                w.WriteStartObject();
                w.WriteNumber("ClusterRadius", ClusterRadius);
                w.WriteNumber("ClusterTimeWindowSec", ClusterTimeWindowSec);
                w.WriteNumber("AlarmWindowSec", AlarmWindowSec);
                w.WriteNumber("AlarmThreshold", AlarmThreshold);
                w.WriteEndObject();
                // Logging keep
                if (doc.RootElement.TryGetProperty("Logging", out var logging))
                {
                    w.WritePropertyName("Logging");
                    logging.WriteTo(w);
                }
                w.WriteEndObject();
                w.Flush();
                File.WriteAllBytes(settingsPath, ms.ToArray());
                StatusText = "Settings saved.";
            }
            catch (Exception ex)
            {
                StatusText = "Settings save error: " + ex.Message;
            }
        }
    }
}

