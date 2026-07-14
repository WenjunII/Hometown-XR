param(
    [ValidateSet("auto", "3080", "4090")]
    [string]$Profile = "auto",

    [switch]$Tune
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $VenvPython)) {
    Push-Location $Root
    try {
        python -m venv .venv
    }
    finally {
        Pop-Location
    }
}

& $VenvPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $VenvPython -m pip install --extra-index-url https://download.pytorch.org/whl/cu121 -r (Join-Path $Root "requirements-lock.txt")
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $VenvPython (Join-Path $Root "main.py") doctor --profile $Profile
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($Tune) {
    & (Join-Path $PSScriptRoot "benchmark.ps1") -Profile $Profile -Quick
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
