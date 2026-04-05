$repoRoot = $PSScriptRoot
$localDeps = Join-Path $repoRoot ".deps"
$parentDeps = Join-Path (Split-Path -Parent $repoRoot) ".deps"

if (Test-Path $localDeps) {
    $env:PYTHONPATH = $localDeps
} elseif (Test-Path $parentDeps) {
    $env:PYTHONPATH = $parentDeps
}

python -m fraud_pipeline.validate_pipeline
