using System;
using System.Diagnostics;
using System.IO;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Markup.Xaml;
using Avalonia.Threading;
using DekibaeCsvAnalyzer.ViewModels;

namespace DekibaeCsvAnalyzer.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        AvaloniaXamlLoader.Load(this);
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
            var dlg = new OpenFolderDialog();
            var path = await dlg.ShowAsync(this);
            if (!string.IsNullOrWhiteSpace(path) && DataContext is MainWindowViewModel vm)
                vm.InputRoot = path;
        };

        if (browseCodebook != null) browseCodebook.Click += async (_, __) =>
        {
            var dlg = new OpenFileDialog { AllowMultiple = false, Filters = { new FileDialogFilter { Name = "CSV", Extensions = { "csv" } } } };
            var paths = await dlg.ShowAsync(this);
            if (paths != null && paths.Length > 0 && DataContext is MainWindowViewModel vm)
                vm.CodebookPath = paths[0];
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

