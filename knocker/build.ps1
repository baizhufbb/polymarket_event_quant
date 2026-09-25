# Builds the knock library:
#   ..\polymarket_bot\libknocker.so  Linux, what the server loads (committed)
#   build\knocker.dll                Windows, for local tests (not committed)
#   build\fakevenue.exe              the stand-in exchange for the Python integration test
# Go's tests run first. Each library is stamped with the hash of the sources
# it was built from, which the Python tests check.
param(
    [string]$Go = 'go',
    [string]$Zig = 'D:\zig\zig.exe'
)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$root = Split-Path -Parent $here

function Invoke-Go([string[]]$arguments) {
    & $Go @arguments
    if ($LASTEXITCODE -ne 0) { throw "go $($arguments -join ' ') failed" }
}

Push-Location $root
try {
    $hash = (& (Join-Path $root '.venv\Scripts\python.exe') -c "from polymarket_bot.knocker import source_hash; print(source_hash())").Trim()
}
finally {
    Pop-Location
}
if ($hash.Length -ne 64) { throw "could not hash the sources" }
$ldflags = "-s -w -X main.sourceHash=$hash"

Push-Location $here
try {
    $env:CGO_ENABLED = '1'
    $env:GOARCH = 'amd64'
    $env:GOOS = 'windows'
    $env:CC = "$Zig cc -target x86_64-windows-gnu"
    Invoke-Go @('vet', './...')
    Invoke-Go @('test', '-count=1', './internal/...')
    Invoke-Go @('build', '-buildmode=c-shared', '-trimpath', '-ldflags', $ldflags, '-o', 'build\knocker.dll', '.')

    $env:GOOS = 'linux'
    $env:CC = "$Zig cc -target x86_64-linux-gnu.2.41"
    Invoke-Go @('build', '-buildmode=c-shared', '-trimpath', '-ldflags', $ldflags, '-o', 'build\libknocker.so', '.')
    Copy-Item 'build\libknocker.so' (Join-Path $root 'polymarket_bot\libknocker.so') -Force

    $env:GOOS = 'windows'
    $env:CGO_ENABLED = '0'
    Invoke-Go @('build', '-trimpath', '-o', 'build\fakevenue.exe', './cmd/fakevenue')
}
finally {
    Pop-Location
}
"built from sources $hash"
