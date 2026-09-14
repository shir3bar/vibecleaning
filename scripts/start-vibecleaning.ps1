[CmdletBinding()]
param(
    [ValidateSet("Rds", "Csv")]
    [string]$Profile = "Rds",

    [string]$DataRoot = "",

    [string]$CacheRoot = "",

    [ValidateRange(1, 65535)]
    [int]$Port = 8422,

    [ValidateSet("required", "disabled")]
    [string]$SharedLocking = "required"
)

$ErrorActionPreference = "Stop"
$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

if (-not $DataRoot) {
    $DataRoot = $env:VIBECLEANING_DATA_ROOT
}
if (-not $DataRoot) {
    throw "Specify -DataRoot or set VIBECLEANING_DATA_ROOT to the university share."
}
if (-not (Test-Path -LiteralPath $DataRoot -PathType Container)) {
    throw "The data root is unavailable or is not a directory: $DataRoot"
}

if (-not $env:LOCALAPPDATA) {
    throw "LOCALAPPDATA is unavailable; a machine-local environment cannot be configured."
}
if (-not $CacheRoot) {
    $CacheRoot = $env:VIBECLEANING_CACHE_ROOT
}
if (-not $CacheRoot) {
    $CacheRoot = Join-Path $env:LOCALAPPDATA "Vibecleaning\cache"
}
if ($CacheRoot.StartsWith("\\")) {
    throw "The cache root must be on this computer, not a UNC/network share: $CacheRoot"
}
New-Item -ItemType Directory -Force -Path $CacheRoot | Out-Null

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw "The 'uv' command was not found. Install uv, open a new PowerShell window, and retry."
}

if (-not $PSBoundParameters.ContainsKey("Port")) {
    $Port = if ($Profile -eq "Rds") { 8422 } else { 8421 }
}

$Probe = [System.Net.Sockets.TcpListener]::new(
    [System.Net.IPAddress]::Loopback,
    $Port
)
try {
    $Probe.Start()
} catch {
    throw "Port $Port on 127.0.0.1 is already in use. Stop the existing instance or choose -Port with a free port."
} finally {
    $Probe.Stop()
}

$EnvironmentRoot = Join-Path $env:LOCALAPPDATA "Vibecleaning\venv"
$EntryPoint = if ($Profile -eq "Rds") {
    Join-Path $RepositoryRoot "examples\rds_movement\server.py"
} else {
    Join-Path $RepositoryRoot "examples\slim_movement\server.py"
}

$env:UV_PROJECT_ENVIRONMENT = $EnvironmentRoot
$env:VIBECLEANING_DATA_ROOT = (Resolve-Path -LiteralPath $DataRoot).Path
$env:VIBECLEANING_CACHE_ROOT = (Resolve-Path -LiteralPath $CacheRoot).Path
$env:VIBECLEANING_SHARED_LOCKING = $SharedLocking
$env:HOST = "127.0.0.1"
$env:PORT = [string]$Port

Push-Location $RepositoryRoot
try {
    Write-Host "Preparing the local Vibecleaning environment at $EnvironmentRoot"
    & uv sync --locked --no-dev
    if ($LASTEXITCODE -ne 0) {
        throw "Dependency installation failed. Check Python 3.11 availability and network/package access."
    }

    Write-Host "Starting $Profile profile at http://127.0.0.1:$Port"
    Write-Host "Authoritative data: $env:VIBECLEANING_DATA_ROOT"
    Write-Host "Disposable cache:  $env:VIBECLEANING_CACHE_ROOT"
    if ($SharedLocking -eq "disabled") {
        Write-Warning "COOPERATIVE MODE: reviewers must coordinate one active writer at a time."
    }
    & uv run --no-sync python $EntryPoint
    $ServerExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $ServerExitCode
