param(
    [ValidateSet("auto", "3080", "4090")]
    [string]$Profile = "auto",

    [switch]$Tune,

    [switch]$Dev
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"

Push-Location $Root
try {
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        throw "Git is required. Install Git and Git LFS before setup."
    }
    git lfs version | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Git LFS is required. Install it, then run 'git lfs install' and 'git lfs pull'."
    }

    if (-not (Test-Path -LiteralPath $VenvPython)) {
        python -m venv .venv
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to create .venv. Install Python 3.10 and try again."
        }
    }

    & $VenvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $VenvPython -m pip install --extra-index-url https://download.pytorch.org/whl/cu121 -r (Join-Path $Root "requirements-lock.txt")
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    if ($Dev) {
        & $VenvPython -m pip install -r (Join-Path $Root "requirements-test.txt")
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }

    & $VenvPython (Join-Path $Root "main.py") doctor --profile $Profile
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    if ($Tune) {
        & (Join-Path $PSScriptRoot "benchmark.ps1") -Profile $Profile -Quick
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
}
finally {
    Pop-Location
}
