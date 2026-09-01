using './main.bicep'

// accountName/location/databaseName match what's actually deployed in
// rg-fabric-medallion-multisourcev2-poc-westus3. principalId is left as a placeholder —
// fill in your own Entra object id (`az ad signed-in-user show --query id -o tsv`) before
// using this file directly; infra/cosmos/setup.ps1 resolves it for you automatically
// instead and doesn't read this file at all.
param accountName = 'cosmosfabricmsv2915d'
param location = 'westus3'
param databaseName = 'multisource'
param principalId = '<your-entra-object-id>'
param publicNetworkAccess = 'Disabled'
param allowedIpRanges = []
