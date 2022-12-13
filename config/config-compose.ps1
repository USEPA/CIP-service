try {
    $scriptPath = $PSScriptRoot
    if (!$scriptPath)
    {
        if ($psISE)
        {
            $scriptPath = Split-Path -Parent -Path $psISE.CurrentFile.FullPath
        } else {
            Write-Host -ForegroundColor Red "Cannot resolve script file's path"
            exit 1
        }
    }
} catch {
    Write-Host -ForegroundColor Red "Caught Exception: $($Error[0].Exception.Message)"
    exit 2
}

$env:CURRENT_UID='1000:1000' 
$env:COMPOSE_PROJECT_NAME="config"
$env:SCRIPT_PATH=$scriptPath
$env:BUNDLE=$args[0]
$env:BPROFILE=$args[1]

& 'docker-compose' '-f' (Join-Path $scriptPath 'config-compose.yml') 'up'
