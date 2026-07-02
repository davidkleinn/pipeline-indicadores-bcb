# load_env.ps1 — carrega o .env como variáveis de ambiente da sessão atual
Get-Content ".env" | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
        $name  = $matches[1].Trim()
        $value = $matches[2].Trim()
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}
Write-Host "Variáveis do .env carregadas na sessão." -ForegroundColor Green