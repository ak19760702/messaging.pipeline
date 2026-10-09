# Demo: package + run against sample In/ next to the project root.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not $env:JAVA_HOME) {
    Write-Host "WARN: JAVA_HOME not set; using java from PATH"
}

Write-Host "==> Building (skip tests)..."
mvn -q -DskipTests package
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$jar = Get-ChildItem -Path "target" -Filter "messaging-pipeline-*.jar" |
    Where-Object { $_.Name -notlike "original-*" } |
    Select-Object -First 1
if (-not $jar) {
    Write-Error "Jar not found under target/"
}

Write-Host "==> Running $($jar.Name) (cwd=$root)..."
& java -jar $jar.FullName
exit $LASTEXITCODE
