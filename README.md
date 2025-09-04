BizCsvAnalyzer (.NET 8 / Avalonia 11-ready core)
=================================================

目的
- Windows 10/11 向け業務アプリの CSV 分析基盤（PathScanner / CsvLoader / Analyzer ほか）
- 出力先: `out/exports`（命名規約に準拠）/ ログ: `out/logs`

前提
- .NET 8 SDK (LTS)
- Linux 上の作業例（WSL/Ubuntu 等）

配置（ユーザー指定パス）
- 例: `~/MyWorks/DekibaeTrendAnalyzer2`
- リポジトリ・ルートは本ディレクトリ `BizCsvAnalyzer/`（Git はこの直下で初期化）

セットアップ手順（Linux）
1) 作業パスに配置
   mkdir -p ~/MyWorks/DekibaeTrendAnalyzer2
   cp -a BizCsvAnalyzer ~/MyWorks/DekibaeTrendAnalyzer2/
   cd ~/MyWorks/DekibaeTrendAnalyzer2/BizCsvAnalyzer

2) Git 初期化
   git init
   git add .
   git commit -m "chore: initial import BizCsvAnalyzer"

3) パッケージ復元・ビルド・テスト
   dotnet build tests/BizCsvAnalyzer.Tests/BizCsvAnalyzer.Tests.csproj -c Release
   dotnet test tests/BizCsvAnalyzer.Tests/BizCsvAnalyzer.Tests.csproj -c Release

4) 設定（必須）
- `src/appsettings.json` を開き、`Paths.InputRoot` を実データの絶対パスに変更
  例: `"InputRoot": "/mnt/d/CIS/Results"`

5) 実行（最小エントリ）
   dotnet run --project src/BizCsvAnalyzer.csproj

出力
- `out/exports/Aggregate_*`, `Cluster_*`, `AlarmRate_*`
- ログ: `out/logs/app_yyyy-MM-dd.log`

注意
- 本リポジトリは UI を最小限（Avalonia のプレースホルダー）に留め、ドメイン/サービス層の動作確認を優先
- 大規模 CSV（10万〜100万行）での計測は `CsvLoader` と `Analyzer` のパラメータ（r, t, Δt）を調整して実施
- Avalonia を導入する場合は、11.x 系パッケージ（`Avalonia`, `Avalonia.Desktop`, `Avalonia.Themes.Fluent` など）を追加し、ターゲットフレームワークは `net8.0` を推奨
