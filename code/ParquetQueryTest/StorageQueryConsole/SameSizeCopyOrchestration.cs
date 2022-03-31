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
            if (config.AdxClusterUri != null && config.AdxDatabase != null)
            {
                foreach (var dataPrepNode in config.DataPrep)
                {
                    await RunAsync(
                        config.AuthenticationMode,
                        config.AdxClusterUri,
                        config.AdxDatabase,
                        dataPrepNode);
                }
            }
        }

        private static async Task RunAsync(
            AuthenticationMode authenticationMode,
            Uri adxClusterUri,
            string adxDatabase,
            DataPrepConfiguration dataPrepNode)
        {
            Console.WriteLine("Copy 'same size'");

            var builder = GetKustoConnectionStringBuilder(adxClusterUri, authenticationMode);
            var kustoCommandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
            var (containerName, blobItems) =
                await GetBlobItemsAsync(authenticationMode, dataPrepNode.OriginDataFolderUri);

            Console.WriteLine($"{blobItems.Count} blobs");

            foreach (var item in blobItems)
            {
                var originPath =
                    $"https://{dataPrepNode.OriginDataFolderUri.Host}/{containerName}/{item.Name}";
                var destinationPath = $"{dataPrepNode.DestinationDataFolderUri}/"
                    + $"{item.Name.Replace(".csv.gz", string.Empty)}";

                await LoadCsvsIntoParquetAsync(
                    kustoCommandProvider,
                    adxDatabase,
                    originPath,
                    destinationPath);
                Console.WriteLine($"Wrote '{destinationPath}'");
            }
        }

        private static async Task LoadCsvsIntoParquetAsync(
            ICslAdminProvider kustoCommandProvider,
            string adxDatabase,
            string originPath,
            string destinationPath)
        {
            await kustoCommandProvider.ExecuteControlCommandAsync(
                adxDatabase,
                @$".export compressed to parquet (
                        h@'{destinationPath};impersonate'
                      ) with(
                        sizeLimit = 1073741824,
                        namePrefix = '1',
                        compressionType = 'snappy',
                        distributed = false,
                        useNativeParquetWriter = true
                      )
                      <|
                      externaldata(Timestamp: datetime, Instance: string, Node: string, Level: string, Component: string, EventId: guid, Detail: string)
                      [
                        h@'{originPath};impersonate'
                      ]
                      with(format = 'csv')");
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