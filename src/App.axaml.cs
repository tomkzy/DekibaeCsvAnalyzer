using System;
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using DekibaeCsvAnalyzer.Views;

namespace DekibaeCsvAnalyzer;

public partial class App : Application
{
    public override void Initialize()
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
            catch { /* give up; minimal app still initializes */ }
        }
    }

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            desktop.MainWindow = new MainWindow();
        }

        base.OnFrameworkInitializationCompleted();
    }
}
