param(
    [string]$Output = "build-elfloader/workerd-memory-ladder.jsonl"
)

$ErrorActionPreference = "Stop"
$sizes = @(256, 320, 384, 448, 512, 640, 768, 1024, 1536, 2048)
$target = if ($env:CARGO_TARGET_DIR) {
    $env:CARGO_TARGET_DIR
} else {
    Join-Path $env:TEMP "hluk-v014-50c2"
}
$binary = Join-Path $target "release/examples/workerd-memory-probe.exe"
$env:CARGO_TARGET_DIR = $target
$env:HYPERLIGHT_MAX_SURROGATES = "2"
$env:HYPERLIGHT_INITIAL_SURROGATES = "0"

cargo build --release --example workerd-memory-probe --locked
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$outputPath = Join-Path (Get-Location) $Output
New-Item -ItemType Directory -Force (Split-Path $outputPath) | Out-Null
Remove-Item $outputPath -ErrorAction SilentlyContinue
foreach ($size in $sizes) {
    Write-Host "==> scratch ${size} MiB"
    & $binary $size 2>> "$outputPath.stderr" |
        Select-Object -Last 1 |
        Tee-Object -FilePath $outputPath -Append
}
