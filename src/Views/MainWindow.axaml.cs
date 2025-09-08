using System;
using System.Diagnostics;
using System.IO;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Markup.Xaml;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using DekibaeCsvAnalyzer.ViewModels;

namespace DekibaeCsvAnalyzer.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        try
        {
            AvaloniaXamlLoader.Load(this);
        }
        catch
        {
            try
            {
                Environment.SetEnvironmentVariable("AVALONIA_LOAD_AS_XAML", "1");
                AvaloniaXamlLoader.Load(this);
            }
            catch { /* fine, window can still show minimal content if needed */ }
        }
        this.DataContext = new MainWindowViewModel();
        WireUiHandlers();
    }

    private void WireUiHandlers()
    {
        // If XAML namescope is missing, skip wiring (should not happen with precompiled XAML)
        if (NameScope.GetNameScope(this) is null) return;

        var browseInput = this.FindControl<Button>("BrowseInputButton");
        var browseCodebook = this.FindControl<Button>("BrowseCodebookButton");
        var openOutput = this.FindControl<Button>("OpenOutputButton");
        var openLogs = this.FindControl<Button>("OpenLogsButton");

        if (browseInput != null) browseInput.Click += async (_, __) =>
        {
            try
            {
                var result = await this.StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
                {
                    AllowMultiple = false
                });
                var folder = (result != null && result.Count > 0) ? result[0] : null;
                if (folder != null && DataContext is MainWindowViewModel vm)
                    vm.InputRoot = folder.Path?.LocalPath ?? folder.TryGetLocalPath() ?? vm.InputRoot;
            }
            catch { }
        };

        if (browseCodebook != null) browseCodebook.Click += async (_, __) =>
        {
            try
            {
                var csvType = new FilePickerFileType("CSV") { Patterns = new[] { "*.csv" } };
                var files = await this.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
                {
                    AllowMultiple = false,
                    FileTypeFilter = new[] { csvType }
                });
                var file = (files != null && files.Count > 0) ? files[0] : null;
                if (file != null && DataContext is MainWindowViewModel vm)
                    vm.CodebookPath = file.Path?.LocalPath ?? file.TryGetLocalPath() ?? vm.CodebookPath;
            }
            catch { }
        };

        if (openOutput != null) openOutput.Click += (_, __) =>
        {
            if (DataContext is MainWindowViewModel vm)
            {
                var p = vm.OutputRoot;
                if (string.IsNullOrWhiteSpace(p)) p = "out";
                try
                {
                    var full = Path.GetFullPath(p);
                    Directory.CreateDirectory(full);
                    OpenInExplorer(full);
                }
                catch { }
            }
        };

        if (openLogs != null) openLogs.Click += (_, __) =>
        {
            if (DataContext is MainWindowViewModel vm)
            {
                var p = vm.OutputRoot;
                if (string.IsNullOrWhiteSpace(p)) p = "out";
                try
                {
                    var full = Path.GetFullPath(Path.Combine(p, "logs"));
                    Directory.CreateDirectory(full);
                    OpenInExplorer(full);
                }
                catch { }
            }
        };
    }

    private void OpenInExplorer(string path)
    {
        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = path,
                UseShellExecute = true,
                Verb = "open"
            });
        }
        catch { }
    }
}
