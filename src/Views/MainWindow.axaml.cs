using System;
using System.Diagnostics;
using System.IO;
using Avalonia.Controls;
using Avalonia.Markup.Xaml;
using DekibaeCsvAnalyzer.ViewModels;
using Avalonia.Layout;
using Avalonia;

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
            // Minimal fallback UI when XAML isn't available
            var panel = new StackPanel { Spacing = 8, Margin = new Thickness(12) };
            panel.Children.Add(new TextBlock { Text = "Dekibae CSV Analyzer", FontSize = 20 });
            panel.Children.Add(new TextBlock { Text = "XAML ロードに失敗したため簡易UIで起動", Opacity = 0.7 });
            Content = panel;
        }
        this.DataContext = new MainWindowViewModel();
        WireUiHandlers();
    }

    private void WireUiHandlers()
    {
        // If no namescope (fallback UI), skip wiring
        if (NameScope.GetNameScope(this) is null)
            return;
        Button? browseInput = null, browseCodebook = null, openOutput = null, openLogs = null;
        try
        {
            browseInput = this.FindControl<Button>("BrowseInputButton");
            browseCodebook = this.FindControl<Button>("BrowseCodebookButton");
            openOutput = this.FindControl<Button>("OpenOutputButton");
            openLogs = this.FindControl<Button>("OpenLogsButton");
        }
        catch
        {
            // Names not found (fallback UI). Skip wiring.
            return;
        }

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
