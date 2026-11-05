param(
    [Parameter(Mandatory=$true)][string]$ConfigPath,
    [Parameter(Mandatory=$true)][string]$CredentialPath,
    [Parameter(Mandatory=$true)][string]$LogDirectory
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo '.venv\Scripts\python.exe'
$entry = Join-Path $repo 'src\memory_http.py'
$config = (Resolve-Path -LiteralPath $ConfigPath).Path
$credential = (Resolve-Path -LiteralPath $CredentialPath).Path
if (-not (Test-Path -LiteralPath $python)) { throw 'Existing OB virtual environment is required' }
$secureToken = (Get-Content -LiteralPath $credential -Raw).Trim() | ConvertTo-SecureString
$pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureToken)
$previous = [Environment]::GetEnvironmentVariable('MEMORY_MCP_HTTP_TOKEN', 'Process')
try {
    $token = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    [Environment]::SetEnvironmentVariable('MEMORY_MCP_HTTP_TOKEN', $token, 'Process')
    $null = New-Item -ItemType Directory -Path $LogDirectory -Force
    $stdout = Join-Path $LogDirectory 'memory-http.stdout.log'
    $stderr = Join-Path $LogDirectory 'memory-http.stderr.log'
    # Token is inherited through the environment, never a command argument.
    $arguments = @('-B', '-u', ('"{0}"' -f $entry), '--config', ('"{0}"' -f $config))
    $process = Start-Process -FilePath $python -ArgumentList $arguments -WorkingDirectory $repo `
        -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
    [PSCustomObject]@{ProcessId=$process.Id;ConfigPath=$config;CredentialPath=$credential}
} finally {
    [Environment]::SetEnvironmentVariable('MEMORY_MCP_HTTP_TOKEN', $previous, 'Process')
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    $token = $null
    $secureToken.Dispose()
}
