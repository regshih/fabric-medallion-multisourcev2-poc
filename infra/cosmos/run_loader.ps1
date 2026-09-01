<#
.SYNOPSIS
Runs cosmos/load_initial.py, cosmos/load_incremental.py, and validation/validate_cosmos.py
for real against the deployed, private-endpoint-only Cosmos DB account, using a short-lived
VM inside the account's VNet.

.DESCRIPTION
With the account's steady-state `publicNetworkAccess: Disabled` (infra/cosmos/main.bicep),
nothing outside the VNet can reach it — that includes an ordinary dev machine. This script
is the answer to "how does anyone actually run the loaders, then": it creates a small,
temporary Ubuntu VM in the account's VNet (subnet `snet-runner-vm`), attaches the
already-provisioned `id-cosmos-loader` user-assigned managed identity (granted Cosmos DB
Built-in Data Contributor — see main.bicep), runs the three scripts on it via
`az vm run-command` (no SSH/public IP needed), prints their output, and deletes the VM
(and its NIC/disk) when done. Nothing is left running.

Two things this repo tried first and abandoned, so you don't have to re-discover them:
  - Azure Container Instances deployed into this VNet cannot reach the instance metadata
    service (confirmed by direct TCP testing: 169.254.169.254:80 timed out on every
    attempt), so managed identity doesn't work for VNet-injected ACI here. A VM does.
  - This VM does NOT need a NAT gateway for outbound internet (apt/pip/PyPI reachability) —
    empirically it already has it. If that ever stops being true in this tenant, see
    infra/cosmos/main.bicep's comments on the (currently unused, still-deployed) snet-runner
    subnet + NAT gateway pattern that was built for the ACI attempt.

No account key is used anywhere. Auth is Entra ID via the attached managed identity.

.EXAMPLE
./run_loader.ps1 -ResourceGroup rg-fabric-medallion-multisourcev2-poc-westus3 -AccountName cosmosfabricmsv2915d
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ResourceGroup,
    [Parameter(Mandatory)][string]$AccountName,
    [string]$VnetName,
    [string]$RunnerVmSubnetName = 'snet-runner-vm',
    [string]$IdentityName = 'id-cosmos-loader',
    [string]$VmName = 'vm-cosmos-loader',
    [string]$VmSize = 'Standard_B2als_v2',
    [string]$DatabaseName = 'multisource',
    [ValidateSet('initial', 'incremental', 'validate', 'all')][string]$Run = 'all'
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
if (-not $VnetName) { $VnetName = "$AccountName-vnet" }

$endpoint = az cosmosdb show -g $ResourceGroup -n $AccountName --query documentEndpoint -o tsv
if (-not $endpoint) { throw "Could not resolve the Cosmos DB endpoint for account $AccountName in $ResourceGroup." }
$identity = az identity show -g $ResourceGroup -n $IdentityName -o json | ConvertFrom-Json
if (-not $identity) { throw "Managed identity $IdentityName not found. Create it and grant it Cosmos DB Built-in Data Contributor first (see infra/cosmos/main.bicep's principalId parameter for the pattern)." }

# Build one Python payload by concatenating the real repo modules (so what runs on the VM
# is the actual committed logic, not a hand-maintained copy) plus a small driver.
function Build-Payload {
    param([string]$RunMode)
    $gen = Get-Content (Join-Path $repoRoot 'generators\generate_cosmos_data.py') -Raw
    $common = Get-Content (Join-Path $repoRoot 'cosmos\common.py') -Raw
    $val = Get-Content (Join-Path $repoRoot 'validation\validate_cosmos.py') -Raw

    $strip = {
        param([string]$text)
        $text = ($text -split "`n" | Where-Object { $_.Trim() -ne 'from __future__ import annotations' }) -join "`n"
        return ($text -split "`nif __name__ == `"__main__`":")[0]
    }
    $gen = & $strip $gen
    $common = & $strip $common
    $valBody = (& $strip $val) -split "`ndef main\(\) -> None:" | Select-Object -First 1
    $valBody = ($valBody -split "`n" | Where-Object { $_.Trim() -notlike 'from cosmos.common import*' -and $_.Trim() -notlike 'sys.path.insert*' -and $_.Trim() -notlike 'from dotenv import*' }) -join "`n"

    $installPrefix = @'
import subprocess, sys
print("=== INSTALLING DEPENDENCIES ===", flush=True)
subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "--no-input", "--break-system-packages",
                        "azure-cosmos==4.16.3", "azure-identity==1.25.3", "Faker==37.12.0", "aiohttp"])
print("=== DEPENDENCIES INSTALLED ===", flush=True)
'@

    $driverParts = @()
    $driverParts += @'
import os
from pathlib import Path
endpoint = os.environ["COSMOS_ENDPOINT"]
database_name = os.environ.get("COSMOS_DATABASE_NAME", "multisource")
root = Path("/tmp/cosmosdata")
print("=== GENERATING SYNTHETIC DATA ===", flush=True)
initial_manifest = write_batch(root, generate_initial(customer_count=25000, device_count=400, session_count=1200, alert_count=150), "initial")
incremental_manifest = write_batch(root, generate_incremental(), "incremental")
print("GENERATED", initial_manifest, incremental_manifest, flush=True)
'@
    if ($RunMode -in @('initial', 'all')) {
        $driverParts += @'
print("=== LOAD_INITIAL ===", flush=True)
print("INITIAL_LOAD_RESULT", load_directory(root / "initial", endpoint=endpoint, database_name=database_name), flush=True)
'@
    }
    if ($RunMode -in @('incremental', 'all')) {
        $driverParts += @'
print("=== LOAD_INCREMENTAL ===", flush=True)
print("INCREMENTAL_LOAD_RESULT", load_directory(root / "incremental", endpoint=endpoint, database_name=database_name), flush=True)
'@
    }
    if ($RunMode -in @('validate', 'all')) {
        $driverParts += @'
print("=== VALIDATE ===", flush=True)
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential
client = CosmosClient(endpoint, credential=DefaultAzureCredential())
database = client.get_database_client(database_name)
ru_tracker = []
validation_results = []
for container_name in CONTAINERS:
    expected = expected_ids(root, container_name)
    result = validate_container(database, container_name, expected, ru_tracker)
    print("VALIDATED", container_name, result, flush=True)
    validation_results.append(result)
import json
print("VALIDATION_SUMMARY", json.dumps({"database": database_name, "containers": validation_results, "totalValidationRU": round(sum(ru_tracker), 2)}), flush=True)
'@
    }
    $driverParts += 'print("=== ALL STEPS PASSED ===", flush=True)'

    return $installPrefix + "`n`n" + $gen + "`n`n" + $common + "`n`n" + $valBody + "`n`n" + ($driverParts -join "`n")
}

$payload = Build-Payload -RunMode $Run
$payloadB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payload))

$scriptLines = @(
    '#!/bin/bash', 'set -e', 'export DEBIAN_FRONTEND=noninteractive',
    'which python3 && python3 -m pip --version || (apt-get update -qq && apt-get install -y -qq python3 python3-pip)',
    "export COSMOS_ENDPOINT=`"$endpoint`"",
    "export COSMOS_DATABASE_NAME=`"$DatabaseName`"",
    "export AZURE_CLIENT_ID=`"$($identity.clientId)`"",
    "cat > /tmp/payload_b64.txt << 'B64EOF'", $payloadB64, 'B64EOF',
    'base64 -d /tmp/payload_b64.txt > /tmp/payload.py', 'python3 /tmp/payload.py'
)
$scriptFile = New-TemporaryFile
Set-Content -Path $scriptFile -Value ($scriptLines -join "`n") -Encoding utf8 -NoNewline

try {
    Write-Host "Creating temporary VM $VmName in $VnetName/$RunnerVmSubnetName ..." -ForegroundColor Yellow
    az vm create -g $ResourceGroup -n $VmName --image Ubuntu2404 --size $VmSize `
        --vnet-name $VnetName --subnet $RunnerVmSubnetName --public-ip-address '""' --nsg '""' `
        --assign-identity $identity.id --generate-ssh-keys --only-show-errors -o none
    if ($LASTEXITCODE -ne 0) { throw 'VM creation failed.' }

    Write-Host 'Running loaders on the VM (this can take a few minutes) ...' -ForegroundColor Yellow
    $result = az vm run-command invoke -g $ResourceGroup -n $VmName --command-id RunShellScript `
        --scripts "@$scriptFile" --only-show-errors -o json | ConvertFrom-Json
    $message = $result.value[0].message
    Write-Host $message
    if ($message -notmatch '=== ALL STEPS PASSED ===') {
        throw 'Loader run did not reach the ALL STEPS PASSED marker — see output above for the failure.'
    }
}
finally {
    Remove-Item $scriptFile -Force -ErrorAction SilentlyContinue
    Write-Host "Deleting temporary VM $VmName and its disk/NIC ..." -ForegroundColor Yellow
    az vm delete -g $ResourceGroup -n $VmName --yes --only-show-errors -o none 2>$null
    az network nic delete -g $ResourceGroup -n "$VmName`VMNic" --only-show-errors -o none 2>$null
    # The OS disk can still show as attached for a few seconds right after `az vm delete`
    # returns, so retry a couple of times rather than leaving it orphaned on a race.
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        $disks = az disk list -g $ResourceGroup --query "[?starts_with(name, '$VmName')].name" -o tsv
        if (-not $disks) { break }
        foreach ($disk in $disks) { az disk delete -g $ResourceGroup -n $disk --yes --only-show-errors -o none 2>$null }
        Start-Sleep -Seconds 10
    }
}
