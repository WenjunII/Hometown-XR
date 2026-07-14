$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    throw "Virtual environment is missing. Run .\scripts\setup.ps1 first."
}

Push-Location $Root
try {
    & $Python -m pytest
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $Python -m ruff check .
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $Python -m compileall -q -x "[\\/](\.venv|data)[\\/]" .
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
    Pop-Location
}
