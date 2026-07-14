param(
    [ValidateSet("auto", "3080", "4090")]
    [string]$Profile = "auto",

    [switch]$Quick,

    [switch]$NoWrite
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    throw "Virtual environment is missing. Run .\scripts\setup.ps1 first."
}

$Arguments = @((Join-Path $Root "main.py"), "benchmark", "--profile", $Profile)
if ($Quick) {
    $Arguments += "--quick"
}
if ($NoWrite) {
    $Arguments += "--no-write"
}

& $Python @Arguments
exit $LASTEXITCODE
