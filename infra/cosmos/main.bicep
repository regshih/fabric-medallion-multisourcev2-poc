// Cosmos DB for NoSQL — source for the Fabric Cosmos DB mirroring leg of this POC.
//
// Networking design (see docs/cosmos-fabric-mirroring.md for the full rationale and
// Microsoft Learn citations):
//   - The account always sits behind a private endpoint in a dedicated VNet. That VNet
//     also carries a pre-created, delegated "gateway subnet" (unused until a Fabric
//     workspace exists) because Fabric's documented private-network mirroring path
//     (Network ACL Bypass + a Fabric virtual network data gateway) needs one, and
//     subnets cost nothing sitting empty.
//   - `publicNetworkAccess` is exposed as a parameter (default 'Disabled') so the
//     narrower single-IP-allowlist mode can be requested for a bootstrap window, mirroring
//     Microsoft's own documented setup procedure ("temporarily enabling Selected
//     networks... only required during setup"). CONFIRMED IN THIS TENANT: requesting
//     'Enabled' (with or without an ipRules allow-list) is silently overridden back to
//     'Disabled' by tenant policy on every deployment — this is the real, tenant-level
//     block described in this repo's task brief, not a guess. The parameter is left in
//     place because it's harmless and documents the attempt, but don't rely on it: the
//     account is private-endpoint-only, always. Because a bootstrap window genuinely
//     isn't available, the loaders instead run from a temporary VM inside this VNet —
//     see infra/cosmos/run_loader.ps1. Local/key auth stays disabled
//     (`disableLocalAuth: true`) regardless, so this is defense in depth either way, not
//     the only thing standing between the account and the internet.
//
// Fabric's Network ACL Bypass authorization (EnableFabricNetworkAclBypass capability +
// networkAclBypassResourceIds pointing at a Fabric workspace ID) is deliberately NOT
// set here — no Fabric workspace exists yet for this POC to authorize. See
// infra/cosmos/enable-fabric-mirroring.ps1, which applies it once a workspace ID is
// available.

@description('Globally unique Cosmos DB account name.')
param accountName string

@description('Deployment region. Must match the Fabric workspace region for mirroring.')
param location string = resourceGroup().location

@description('Cosmos SQL database name.')
param databaseName string = 'multisource'

@description('Principal (object) ID to grant the Cosmos DB Built-in Data Contributor data-plane role. Typically the deploying user or a loader identity.')
param principalId string

@description('Steady state is Disabled (private-endpoint-only) and always will be: requesting Enabled is confirmed to be silently overridden back to Disabled by tenant policy on deployment. Kept as a parameter only to document that this was tried.')
@allowed(['Enabled', 'Disabled'])
param publicNetworkAccess string = 'Disabled'

@description('IP addresses/CIDRs allowed when publicNetworkAccess is Enabled. Ignored when Disabled. Keep this to the minimum needed (e.g. a single dev-machine IP), never 0.0.0.0/0.')
param allowedIpRanges array = []

@description('Address space for the dedicated Cosmos networking VNet.')
param vnetAddressPrefix string = '10.60.0.0/16'

@description('Private endpoint subnet prefix.')
param privateEndpointSubnetPrefix string = '10.60.1.0/24'

@description('Reserved, delegated subnet for a future Fabric virtual network data gateway. Not used until a Fabric workspace exists; costs nothing empty. Minimum /27 per Fabric docs.')
param fabricGatewaySubnetPrefix string = '10.60.2.0/27'

@description('Delegated subnet originally reserved for a data-loader container instance. Azure Container Instances deployed into this VNet could not reach the instance metadata service (IMDS) needed for managed identity — confirmed by direct testing (TCP connect to 169.254.169.254 timed out every time), not just a config mistake — so this approach was abandoned in favor of a VM (see runnerVmSubnetPrefix below). Left deployed as-is because Azure was still cleaning up the container-group service-association link at the time this template was finalized, which blocks removing the delegation; safe to delete by hand once that clears. Not used by anything.')
param runnerSubnetPrefix string = '10.60.3.0/28'

@description('Plain (undelegated) subnet for a temporary bootstrap VM that runs the JSON document loaders (see infra/cosmos/run_loader.ps1). With publicNetworkAccess Disabled, this is the only way to run them at all. Costs nothing while no VM is deployed into it.')
param runnerVmSubnetPrefix string = '10.60.4.0/28'

var cosmosDataContributorRoleId = '00000000-0000-0000-0000-000000000002'
var privateDnsZoneName = 'privatelink.documents.azure.com'

resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: '${accountName}-vnet'
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [vnetAddressPrefix]
    }
    subnets: [
      {
        name: 'snet-pe'
        properties: {
          addressPrefix: privateEndpointSubnetPrefix
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
      {
        name: 'snet-fabric'
        properties: {
          addressPrefix: fabricGatewaySubnetPrefix
          delegations: [
            {
              name: 'fabric-vnet-gateway'
              properties: {
                serviceName: 'Microsoft.PowerPlatform/vnetaccesslinks'
              }
            }
          ]
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
      {
        name: 'snet-runner'
        properties: {
          addressPrefix: runnerSubnetPrefix
          delegations: [
            {
              name: 'aci-delegation'
              properties: {
                serviceName: 'Microsoft.ContainerInstance/containerGroups'
              }
            }
          ]
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
      {
        name: 'snet-runner-vm'
        properties: {
          addressPrefix: runnerVmSubnetPrefix
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
}

resource account 'Microsoft.DocumentDB/databaseAccounts@2025-04-15' = {
  name: accountName
  location: location
  kind: 'GlobalDocumentDB'
  properties: {
    capabilities: [
      { name: 'EnableServerless' }
    ]
    databaseAccountOfferType: 'Standard'
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    backupPolicy: {
      type: 'Continuous'
      continuousModeProperties: {
        tier: 'Continuous7Days'
      }
    }
    disableLocalAuth: true
    disableKeyBasedMetadataWriteAccess: true
    minimalTlsVersion: 'Tls12'
    publicNetworkAccess: publicNetworkAccess
    isVirtualNetworkFilterEnabled: false
    ipRules: [for ip in allowedIpRanges: { ipAddressOrRange: ip }]
    networkAclBypass: 'None'
    networkAclBypassResourceIds: []
  }
}

resource privateDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: privateDnsZoneName
  location: 'global'
}

resource privateDnsZoneVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: privateDnsZone
  name: '${accountName}-vnet-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2024-05-01' = {
  name: '${accountName}-pe'
  location: location
  properties: {
    subnet: {
      id: vnet.properties.subnets[0].id
    }
    privateLinkServiceConnections: [
      {
        name: '${accountName}-plsc'
        properties: {
          privateLinkServiceId: account.id
          groupIds: ['Sql']
        }
      }
    ]
  }
}

resource privateEndpointDnsGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-05-01' = {
  parent: privateEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'documents-azure-com'
        properties: {
          privateDnsZoneId: privateDnsZone.id
        }
      }
    ]
  }
}

resource database 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2025-04-15' = {
  parent: account
  name: databaseName
  properties: {
    resource: {
      id: databaseName
    }
    options: {}
  }
}

var containerNames = ['digitalSessions', 'devices', 'fraudAlerts']

resource containers 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2025-04-15' = [for name in containerNames: {
  parent: database
  name: name
  properties: {
    resource: {
      id: name
      partitionKey: {
        paths: ['/customerId']
        kind: 'Hash'
        version: 2
      }
      indexingPolicy: {
        indexingMode: 'consistent'
        automatic: true
        includedPaths: [{ path: '/*' }]
        excludedPaths: [{ path: '/"_etag"/?' }]
      }
    }
  }
}]

resource dataContributorAssignment 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2025-04-15' = {
  parent: account
  name: guid(account.id, principalId, cosmosDataContributorRoleId)
  properties: {
    roleDefinitionId: '${account.id}/sqlRoleDefinitions/${cosmosDataContributorRoleId}'
    principalId: principalId
    scope: account.id
  }
}

output accountName string = account.name
output endpoint string = account.properties.documentEndpoint
output accountId string = account.id
output database string = database.name
output backupMode string = account.properties.backupPolicy.type
output publicNetworkAccess string = account.properties.publicNetworkAccess
output vnetId string = vnet.id
output vnetName string = vnet.name
output fabricGatewaySubnetId string = vnet.properties.subnets[1].id
output runnerSubnetId string = vnet.properties.subnets[2].id
output runnerVmSubnetId string = vnet.properties.subnets[3].id
output privateEndpointId string = privateEndpoint.id
