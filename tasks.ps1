<#
.SYNOPSIS
    Windows equivalent of the Makefile. `make` is not installed by default on
    Windows, and the graders may be on any platform, so both exist and stay in
    sync.

.EXAMPLE
    .\tasks.ps1 install
    .\tasks.ps1 run
    .\tasks.ps1 verify
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'install', 'migrate', 'run', 'test', 'lint', 'check',
                 'restart-check', 'verify', 'bench-engine', 'load', 'load-burst',
                 'smoke', 'baseline', 'burst', 'offline', 'adversarial', 'clean')]
    [string]$Task = 'help'
)

$ErrorActionPreference = 'Stop'
$Root = $PSScriptRoot
$Venv = Join-Path $Root '.venv'
$Py = Join-Path $Venv 'Scripts\python.exe'
if (-not (Test-Path $Py)) { $Py = 'python' }

$ServiceUrl = if ($env:SERVICE_URL) { $env:SERVICE_URL } else { 'http://localhost:8080' }
$Devices = if ($env:DEVICES) { $env:DEVICES } else { '50' }

# eval/check.py prints U+26A0 / U+2713, which the default Windows console
# codepage (cp1252) cannot encode. This is the workaround; the harness is
# upstream's file and is left unmodified.
$env:PYTHONIOENCODING = 'utf-8'

function Invoke-Scenario([string]$Name) {
    & $Py (Join-Path $Root 'eval\check.py') $Name --target $ServiceUrl --devices $Devices
}

switch ($Task) {
    'help' {
        Write-Output @'
Setup
  install        create .venv and install runtime + dev dependencies
  migrate        apply database migrations

Run
  run            start the service on :8080 (dual-stack)

Verify
  test           unit tests (56 cases, no server needed)
  restart-check  hard-kill the service and verify recovery end to end
  verify         test + restart-check + smoke
  lint           ruff check
  check          django system check

Measure
  bench-engine   engine throughput without HTTP
  load           end-to-end load + alarm-feed SLA (batch 500)
  load-burst     single-event requests, as the graded generator sends

Challenge harness
  smoke | baseline | burst | offline | adversarial

Override with $env:SERVICE_URL and $env:DEVICES.
'@
    }
    'install' {
        if (-not (Test-Path (Join-Path $Venv 'Scripts\python.exe'))) {
            Write-Output 'creating .venv'
            & py -3.13 -m venv $Venv
        }
        $Py = Join-Path $Venv 'Scripts\python.exe'
        & $Py -m pip install --upgrade pip
        & $Py -m pip install -r (Join-Path $Root 'requirements-dev.txt')
        & $Py (Join-Path $Root 'manage.py') migrate --no-input
    }
    'migrate'       { & $Py (Join-Path $Root 'manage.py') migrate --no-input }
    'run'           { & $Py (Join-Path $Root 'manage.py') migrate --no-input; & $Py (Join-Path $Root 'run.py') }
    'test'          { & $Py -m unittest discover -s tests -t . -v }
    'lint'          { & $Py -m ruff check $Root }
    'check'         { & $Py (Join-Path $Root 'manage.py') check }
    'restart-check' { & $Py -m client.restart_check }
    'verify' {
        & $Py -m unittest discover -s tests -t .
        if ($LASTEXITCODE -ne 0) { throw 'unit tests failed' }
        & $Py -m client.restart_check
        if ($LASTEXITCODE -ne 0) { throw 'restart check failed' }
        Invoke-Scenario 'smoke'
    }
    'bench-engine'  { & $Py -m client.engine_bench }
    'load'          { & $Py -m client.loadgen --rate 200000 --duration 20 --devices 5000 --batch 500 --concurrency 4 }
    'load-burst'    { & $Py -m client.loadgen --rate 50000 --duration 20 --devices 5000 --batch 1 --concurrency 32 }
    'smoke'         { Invoke-Scenario 'smoke' }
    'baseline'      { Invoke-Scenario 'baseline' }
    'burst'         { Invoke-Scenario 'burst' }
    'offline'       { Invoke-Scenario 'offline' }
    'adversarial'   { Invoke-Scenario 'adversarial' }
    'clean' {
        foreach ($dir in @('data', 'data-restart-check', '.ruff_cache')) {
            Remove-Item -Recurse -Force (Join-Path $Root $dir) -ErrorAction SilentlyContinue
        }
        Get-ChildItem -Path $Root -Filter __pycache__ -Recurse -Directory -ErrorAction SilentlyContinue |
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    }
}
