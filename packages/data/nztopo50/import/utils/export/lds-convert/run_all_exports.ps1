$ErrorActionPreference = "Stop"

$extents = @(
    "new_zealand_mainland",
    "chatham_islands",
    "snares_islands",
    "auckland_islands",
    "antipodes_islands",
    "bounty_islands",
    "kermadec_islands",
    "campbell_islands"
)

Push-Location $PSScriptRoot
try {
    foreach ($extent in $extents) {
        uv run python export_cli.py $extent local --output-path C:\Temp\export\lds_model
        if ($LASTEXITCODE -ne 0) {
            throw "Export failed for $extent with exit code $LASTEXITCODE"
        }
    }
}
finally {
    Pop-Location
}
