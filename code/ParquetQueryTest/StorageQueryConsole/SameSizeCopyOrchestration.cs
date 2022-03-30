using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Kusto.Data;
using Kusto.Data.Net.Client;
using StorageQueryConsole.Config;
using System.Collections.Immutable;

namespace StorageQueryConsole
{
    internal class SameSizeCopyOrchestration
    {
        internal static async Task RunAsync(RootConfiguration config)
        {
            if (config.OriginDataFolder != null
                && config.AdxClusterUri != null
                && config.AdxDatabase != null)
            {
                await RunAsync(
                    config.AuthenticationMode,
                    config.OriginDataFolder,
                    config.AdxClusterUri,
                    config.AdxDatabase);
            }
        }

        private static async Task RunAsync(
            AuthenticationMode authenticationMode,
            string originDataFolder,
            Uri adxClusterUri,
            string adxDatabase)
        {
            Console.WriteLine("Copy 'same size'");

            var builder = GetKustoConnectionStringBuilder(adxClusterUri, authenticationMode);
            var kustoCommandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
            var blobItems = await GetBlobItemsAsync(authenticationMode, originDataFolder);

            Console.WriteLine($"{blobItems.Count} blobs");

            //  Create staging table
            await kustoCommandProvider.ExecuteControlCommandAsync(
                adxDatabase,
                @".execute database script <|
                .create-merge table Staging(Timestamp:datetime,Instance:string,Node:string,Level:string,Component:string,EventId:guid,Detail:string)
                .alter table [table_name] policy merge
                ```
                {
                  'AllowRebuild': false,
                  'AllowMerge': false
                }
                ```
                .clear table Staging data");

            throw new NotImplementedException();
        }

        private static async Task<IImmutableList<BlobItem>> GetBlobItemsAsync(
            AuthenticationMode authenticationMode,
            string originDataFolder)
        {
            var originUri = new Uri(originDataFolder);
            var serviceUri = new Uri($"https://{originUri.Host}");
            var pathParts = originUri.LocalPath.Split('/').Skip(1);
            var containerName = pathParts.First();
            var containerPath = string.Join('/', pathParts.Skip(1));
            var credential = GetStorageCredentials(authenticationMode);
            var blobClient = new BlobServiceClient(serviceUri, credential);
            var containerClient = blobClient.GetBlobContainerClient(containerName);
            var blobItems = await containerClient.GetBlobsAsync(prefix: containerPath).ToListAsync();

            return blobItems;
        }

        private static KustoConnectionStringBuilder GetKustoConnectionStringBuilder(
            Uri adxClusterUri,
            AuthenticationMode authenticationMode)
        {
            var builder = new KustoConnectionStringBuilder(adxClusterUri.ToString());

            switch (authenticationMode)
            {
                case AuthenticationMode.AzCli:
                    return builder.WithAadAzCliAuthentication();
                case AuthenticationMode.Browser:
                    return builder.WithAadUserPromptAuthentication();

                default:
                    throw new NotSupportedException(
                        $"Authentication mode not supported:  '{authenticationMode}'");
            }
        }

        private static TokenCredential GetStorageCredentials(AuthenticationMode authenticationMode)
        {
            switch (authenticationMode)
            {
                case AuthenticationMode.AzCli:
                    return new AzureCliCredential();
                case AuthenticationMode.Browser:
                    return new InteractiveBrowserCredential(new InteractiveBrowserCredentialOptions
                    {
                        TokenCachePersistenceOptions = new TokenCachePersistenceOptions()
                    });

                default:
                    throw new NotSupportedException(
                        $"Authentication mode not supported:  '{authenticationMode}'");
            }
        }
    }
}