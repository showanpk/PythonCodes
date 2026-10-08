param(
    [ValidateSet('preview','commit','test-connection')]
    [string]$Mode = 'preview'
)
$ErrorActionPreference = 'Stop'
$folder = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $folder 'sync_innerva_to_crm.py'

Write-Host "`nSAHELI CRM - INNERVA MANUAL SYNC" -ForegroundColor Cyan
Write-Host "Mode: $Mode" -ForegroundColor Yellow
Write-Host "Other Saheli activity sessions will not be changed."

$excel = $null
if ($Mode -ne 'test-connection') {
    $workbooks = @(Get-ChildItem -Path $folder -Filter 'Innerva Booking Sheet*.xlsx' -File)
    if ($workbooks.Count -eq 0) {
        Write-Host 'Place the latest Innerva Booking Sheet.xlsx next to this script first.' -ForegroundColor Red
        exit 2
    }
    if ($workbooks.Count -ne 1) {
        Write-Host 'More than one Innerva Excel file found. Please keep only the current one in this folder.' -ForegroundColor Red
        $workbooks | ForEach-Object { Write-Host $_.Name }
        exit 2
    }
    $excel = $workbooks[0].FullName
    Write-Host "Workbook: $($workbooks[0].Name)"
}

# SQL password is entered temporarily and is never saved in the .py or .ps1 file.
if ([string]::IsNullOrWhiteSpace($env:SAHELI_SQL_CONNECTION_STRING)) {
    # Ask directly in this terminal. Windows credential dialog can open behind other windows.
    Write-Host "`nAzure SQL login required (the password will not be displayed)." -ForegroundColor Yellow
    $enteredUsername = Read-Host 'Azure SQL username'
    if ([string]::IsNullOrWhiteSpace($enteredUsername)) { throw 'Username cannot be blank.' }
    $securePassword = Read-Host 'Azure SQL password' -AsSecureString
    $cred = [System.Management.Automation.PSCredential]::new($enteredUsername, $securePassword)
    $username = $cred.UserName.Replace('}', '}}')
    $password = $cred.GetNetworkCredential().Password.Replace('}', '}}')
    $env:SAHELI_SQL_CONNECTION_STRING = ('Driver={ODBC Driver 18 for SQL Server};' +
        'Server=tcp:sahelihub.database.windows.net,1433;' +
        'Database=SaheliHubCRM;' +
        'Uid={' + $username + '};Pwd={' + $password + '};' +
        'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
}

if ($Mode -eq 'test-connection') {
    Write-Host "`nStarting Azure SQL connection test (no Excel scan, no database changes)..." -ForegroundColor Cyan
} else {
    Write-Host "`nStarting Innerva $Mode (Excel scan may take some time)..." -ForegroundColor Cyan
}
# Assignment inside each branch preserves the array type.
# A single-item array assigned via `= if (...)` can become a string,
# and splatting that string sends each character to Python separately.
if ($Mode -eq 'test-connection') {
    $scriptArgs = @('--test-connection')
} else {
    $scriptArgs = @('--excel', $excel, "--$Mode")
}
try {
    $python = Get-Command py -ErrorAction SilentlyContinue
    if ($python) {
        & py -3 $script @scriptArgs
    } else {
        $python = Get-Command python -ErrorAction SilentlyContinue
        if (-not $python) { throw 'Python was not found. Install Python 3.10+.' }
        & python $script @scriptArgs
    }
    $result = $LASTEXITCODE
} finally {
    Remove-Item Env:SAHELI_SQL_CONNECTION_STRING -ErrorAction SilentlyContinue
    $password = $null
    $securePassword = $null
    $cred = $null
}
exit $result
