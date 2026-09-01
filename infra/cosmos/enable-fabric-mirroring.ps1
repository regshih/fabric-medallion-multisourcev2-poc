<#
.SYNOPSIS
Authorizes a Fabric workspace to reach this private-endpoint-only Cosmos DB account via
Network ACL Bypass, for private-network Fabric mirroring.

.DESCRIPTION
NOT YET RUN as of this writing — it requires a Fabric workspace ID, and no Fabric
workspace exists for this POC yet (workspace creation is out of the Cosmos specialist's
scope; see infra/fabric/provision.py). Run this once that workspace exists.

Implements steps 3-5 of Microsoft's documented private-network mirroring setup
(https://learn.microsoft.com/en-us/fabric/mirroring/azure-cosmos-db-private-network):
  3. Grant the Cosmos DB data-plane permissions Fabric mirroring needs
     (readMetadata + readAnalytics, via a custom role) to the identity that
     creates the Fabric connection.
  4. Enable the EnableFabricNetworkAclBypass capability on the account.
  5. Authorize the specific Fabric workspace ID as a trusted resource
     (networkAclBypass = AzureServices + networkAclBypassResourceIds).

This script does NOT perform steps 1-2 (this repo's infra/cosmos/setup.ps1 already
creates the account, private endpoint, and the reserved "snet-fabric" gateway subnet)
or steps 6-8, which are Fabric-portal/REST-API actions requiring a live Fabric virtual
network data gateway attached to snet-fabric (plus a NAT gateway on that subnet, since
default outbound internet access for new subnets was retired after March 31, 2026) —
see docs/cosmos-fabric-mirroring.md for the full remaining checklist.

No account keys are used. This script only grants Entra ID data-plane RBAC and flips
account-level networking capabilities.

.EXAMPLE
./enable-fabric-mirroring.ps1 -ResourceGroup rg-fabric-medallion-multisourcev2-poc-westus3 `
    -AccountName cosmosfabricmsv2915d -FabricWorkspaceId <workspace-guid> -CallerPrincipalId <object-id>
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ResourceGroup,
    [Parameter(Mandatory)][string]$AccountName,
    [Parameter(Mandatory)][string]$FabricWorkspaceId,
    [string]$CallerPrincipalId
)

$ErrorActionPreference = 'Stop'

if (-not $CallerPrincipalId) {
    $CallerPrincipalId = az ad signed-in-user show --query id --output tsv 2>$null
}
if (-not $CallerPrincipalId) {
    throw 'Could not infer a signed-in user. Pass -CallerPrincipalId for the identity that will create the Fabric connection (Step 7 in the docs).'
}
$tenantId = az account show --query tenantId --output tsv
if (-not $tenantId) { throw 'Could not determine the current tenant ID.' }

# --- Step 3: custom data-plane role for Fabric mirroring metadata/analytics reads ---
$roleName = 'Fabric Mirroring Metadata Reader'
$existingRole = az cosmosdb sql role definition list -a $AccountName -g $ResourceGroup `
    --query "[?roleName=='$roleName'].id | [0]" --output tsv
if (-not $existingRole) {
    az cosmosdb sql role definition create -a $AccountName -g $ResourceGroup --body (@{
        RoleName         = $roleName
        Type             = 'CustomRole'
        AssignableScopes = @('/')
        Permissions      = @(@{ DataActions = @(
            'Microsoft.DocumentDB/databaseAccounts/readMetadata',
            'Microsoft.DocumentDB/databaseAccounts/readAnalytics'
        ) })
    } | ConvertTo-Json -Depth 10 -Compress)
    if ($LASTEXITCODE -ne 0) { throw 'Failed to create the Fabric Mirroring Metadata Reader role definition.' }
    $existingRole = az cosmosdb sql role definition list -a $AccountName -g $ResourceGroup `
        --query "[?roleName=='$roleName'].id | [0]" --output tsv
}
az cosmosdb sql role assignment create -a $AccountName -g $ResourceGroup --scope '/' `
    --principal-id $CallerPrincipalId --role-definition-id $existingRole --only-show-errors | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Failed to assign the Fabric Mirroring Metadata Reader role.' }

# --- Step 4: enable the Network ACL Bypass capability (preserving existing capabilities) ---
$accountResource = Get-AzResource -ResourceGroupName $ResourceGroup -Name $AccountName `
    -ResourceType 'Microsoft.DocumentDB/databaseAccounts'
if ($accountResource.Properties.capabilities.name -notcontains 'EnableFabricNetworkAclBypass') {
    $accountResource.Properties.capabilities += @{ name = 'EnableFabricNetworkAclBypass' }
    $accountResource | Set-AzResource -UsePatchSemantics -Force | Out-Null
}

# --- Step 5: authorize this specific Fabric workspace as a trusted resource ---
$bypassResourceId = "/tenants/$tenantId/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/Fabric/providers/Microsoft.Fabric/workspaces/$FabricWorkspaceId"
az cosmosdb update -g $ResourceGroup -n $AccountName --network-acl-bypass AzureServices `
    --network-acl-bypass-resource-ids $bypassResourceId --only-show-errors | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Failed to authorize the Fabric workspace as a trusted resource.' }

Write-Host "Network ACL Bypass authorized for Fabric workspace $FabricWorkspaceId." -ForegroundColor Green
Write-Host 'Remaining manual steps (Fabric portal / REST API — see docs/cosmos-fabric-mirroring.md):' -ForegroundColor Yellow
Write-Host '  6. Create a Fabric virtual network data gateway on the snet-fabric subnet (attach a NAT gateway to it first).'
Write-Host '  7. Create an Azure Cosmos DB v2 connection (OAuth 2.0) through that gateway.'
Write-Host '  8. Create the mirrored database via the Fabric REST API (the portal UI cannot use a VNet gateway connection) and call startMirroring.'
