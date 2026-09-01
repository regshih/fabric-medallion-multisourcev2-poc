<#
.SYNOPSIS
Deploys the Cosmos DB for NoSQL account (private-endpoint networking, continuous
backup, AAD-only auth) and grants passwordless data-plane access.

.DESCRIPTION
Uses the current Azure CLI context (run `az login` first). Deploys infra/cosmos/main.bicep,
which creates the VNet + private endpoint + private DNS zone + the account itself.

-PublicNetworkAccess / -AllowMyIp exist so a bootstrap window can be REQUESTED, but in this
tenant that request is confirmed to be silently overridden back to Disabled by tenant
policy no matter what — the account never actually opens up. That isn't a bug in this
script; it's the real blocker docs/cosmos-fabric-mirroring.md describes. Because of that,
there is no working way to reach this account from outside its VNet: run
infra/cosmos/run_loader.ps1 (a temporary VM inside the VNet) to actually execute the data
loaders, not by trying to open this account to the internet.

No account keys are read, emitted, or used anywhere in this script.

.EXAMPLE
./setup.ps1 -SubscriptionId <sub> -ResourceGroup <rg> -AccountName <name> -PublicNetworkAccess Disabled
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$SubscriptionId,
    [Parameter(Mandatory)][string]$ResourceGroup,
    [Parameter(Mandatory)][string]$AccountName,
    [string]$Location = 'westus3',
    [string]$DatabaseName = 'multisource',
    [string]$PrincipalId,
    [ValidateSet('Enabled', 'Disabled')][string]$PublicNetworkAccess = 'Disabled',
    [string[]]$AllowedIpRanges = @(),
    [switch]$AllowMyIp
)

$ErrorActionPreference = 'Stop'
$template = Join-Path $PSScriptRoot 'main.bicep'

az account set --subscription $SubscriptionId
if ($LASTEXITCODE -ne 0) { throw 'Azure CLI authentication/subscription selection failed. Run az login and retry.' }

az group create --name $ResourceGroup --location $Location --only-show-errors | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Resource group creation failed.' }

if (-not $PrincipalId) {
    $PrincipalId = az ad signed-in-user show --query id --output tsv 2>$null
}
if (-not $PrincipalId) {
    throw 'Could not infer a signed-in user. Pass -PrincipalId for the loader identity.'
}

if ($AllowMyIp) {
    if ($PublicNetworkAccess -ne 'Enabled') {
        throw '-AllowMyIp only makes sense with -PublicNetworkAccess Enabled.'
    }
    $myIp = (Invoke-RestMethod -Uri 'https://api.ipify.org').Trim()
    if (-not $myIp) { throw 'Could not determine this machine''s public IP.' }
    $AllowedIpRanges = @($myIp)
    Write-Host "Allow-listing this machine's public IP only: $myIp" -ForegroundColor Yellow
}

if ($PublicNetworkAccess -eq 'Enabled' -and $AllowedIpRanges.Count -eq 0) {
    throw 'Refusing to deploy with PublicNetworkAccess=Enabled and no IP allow-list (would be open to the internet). Pass -AllowedIpRanges or -AllowMyIp.'
}

$paramsJson = @{
    accountName          = @{ value = $AccountName }
    location             = @{ value = $Location }
    databaseName         = @{ value = $DatabaseName }
    principalId          = @{ value = $PrincipalId }
    publicNetworkAccess  = @{ value = $PublicNetworkAccess }
    allowedIpRanges      = @{ value = $AllowedIpRanges }
} | ConvertTo-Json -Depth 10 -Compress
$paramsFile = New-TemporaryFile
Set-Content -Path $paramsFile -Value $paramsJson -Encoding utf8

try {
    $deployment = az deployment group create `
        --resource-group $ResourceGroup `
        --template-file $template `
        --parameters "@$paramsFile" `
        --name "cosmos-multisource-poc-$(Get-Date -Format yyyyMMddHHmmss)" `
        --only-show-errors `
        --output json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'Cosmos DB deployment failed.' }
}
finally {
    Remove-Item $paramsFile -Force -ErrorAction SilentlyContinue
}

$outputs = $deployment.properties.outputs
[pscustomobject]@{
    AccountName         = $outputs.accountName.value
    DatabaseName        = $outputs.database.value
    Endpoint            = $outputs.endpoint.value
    BackupMode          = $outputs.backupMode.value
    PublicNetworkAccess = $outputs.publicNetworkAccess.value
    AllowedIpRanges     = $AllowedIpRanges
    VnetName            = $outputs.vnetName.value
    FabricGatewaySubnetId = $outputs.fabricGatewaySubnetId.value
    PrincipalId         = $PrincipalId
} | ConvertTo-Json
