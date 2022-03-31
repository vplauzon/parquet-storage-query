using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using StorageQueryConsole.Config;
using System.Collections.Immutable;

namespace StorageQueryConsole
{
    internal class SameSizeCopyOrchestration
    {
        internal static async Task RunAsync(RootConfiguration config)
        {
            if (config.OriginDataFolderUri != null
                && config.SameSizeDataFolderUri != null
                && config.AdxClusterUri != null
                && config.AdxDatabase != null)
            {
                await RunAsync(
                    config.AuthenticationMode,
                    config.OriginDataFolderUri,
                    config.SameSizeDataFolderUri,
                    config.AdxClusterUri,
                    config.AdxDatabase);
            }
        }

        private static async Task RunAsync(
            AuthenticationMode authenticationMode,
            Uri originDataFolderUri,
            Uri destinationDataFolderUri,
            Uri adxClusterUri,
            string adxDatabase)
        {
            Console.WriteLine("Copy 'same size'");

            var builder = GetKustoConnectionStringBuilder(adxClusterUri, authenticationMode);
            var kustoCommandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
            var (containerName, blobItems) =
                await GetBlobItemsAsync(authenticationMode, originDataFolderUri);

            Console.WriteLine($"{blobItems.Count} blobs");
            await CreateStagingTableAsync(adxDatabase, kustoCommandProvider);
            Console.WriteLine($"Staging table created");

            foreach (var item in blobItems)
            {
                var originPath = $"https://{originDataFolderUri.Host}/{containerName}/{item.Name}";
                var destinationPath =
                    $"{destinationDataFolderUri}/{item.Name.Replace("csv.gz", "parquet")}";
                var tag = $"\"{item.Name}\"";

                await kustoCommandProvider.ExecuteControlCommandAsync(
                    adxDatabase,
                    @$".execute database script <|
                    .append Staging with (tags='[{tag}]') <|
                    externaldata(Timestamp: datetime, Instance: string, Node: string, Level: string, Component: string, EventId: guid, Detail: string)
                    [
                      h@'{originPath};impersonate'
                    ]
                    with(format = 'csv')");
            }
        }

        private static async Task CreateStagingTableAsync(
            string adxDatabase,
            ICslAdminProvider kustoCommandProvider)
        {
            await kustoCommandProvider.ExecuteControlCommandAsync(
                adxDatabase,
                @".execute database script <|
                .create-merge table Staging(Timestamp:datetime,Instance:string,Node:string,Level:string,Component:string,EventId:guid,Detail:string)

                .alter table Staging policy merge
                ```
                {
                  'AllowRebuild': false,
                  'AllowMerge': false
                }
                ```

                .clear table Staging data;");
        }

        private static async Task<(string containerName, IImmutableList<BlobItem> items)> GetBlobItemsAsync(
            AuthenticationMode authenticationMode,
            Uri originDataFolderUri)
        {
            var serviceUri = new Uri($"https://{originDataFolderUri.Host}");
            var pathParts = originDataFolderUri.LocalPath.Split('/').Skip(1);
            var containerName = pathParts.First();
            var containerPath = string.Join('/', pathParts.Skip(1));
            var credential = GetStorageCredentials(authenticationMode);
            var blobClient = new BlobServiceClient(serviceUri, credential);
            var containerClient = blobClient.GetBlobContainerClient(containerName);
            var blobItems = await containerClient.GetBlobsAsync(prefix: containerPath).ToListAsync();

            return (containerName, blobItems);
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