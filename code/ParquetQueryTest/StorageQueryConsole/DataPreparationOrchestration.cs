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
    internal class DataPreparationOrchestration
    {
        internal static async Task RunAsync(
            ICslAdminProvider kustoCommandProvider,
            TokenCredential storageCredential,
            RootConfiguration config)
        {
            if (config.AdxClusterUri != null && config.AdxDatabase != null)
            {
                if (config.DataPrep.Any())
                {
                    Console.WriteLine("Data preparation...");
                    Console.WriteLine();

                    foreach (var dataPrepNode in config.DataPrep)
                    {
                        await RunAsync(
                            kustoCommandProvider,
                            storageCredential,
                            config.AdxClusterUri,
                            config.AdxDatabase,
                            dataPrepNode);
                    }
                }
            }
        }

        private static async Task RunAsync(
            ICslAdminProvider kustoCommandProvider,
            TokenCredential storageCredential,
            Uri adxClusterUri,
            string adxDatabase,
            DataPrepConfiguration dataPrepNode)
        {
            var (containerName, blobItems) =
                await GetBlobItemsAsync(storageCredential, dataPrepNode.OriginDataFolderUri);

            Console.WriteLine($"From {dataPrepNode.OriginDataFolderUri}:  {blobItems.Count} blobs");

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
            TokenCredential storageCredential,
            Uri originDataFolderUri)
        {
            var serviceUri = new Uri($"https://{originDataFolderUri.Host}");
            var pathParts = originDataFolderUri.LocalPath.Split('/').Skip(1);
            var containerName = pathParts.First();
            var containerPath = string.Join('/', pathParts.Skip(1));
            var blobClient = new BlobServiceClient(serviceUri, storageCredential);
            var containerClient = blobClient.GetBlobContainerClient(containerName);
            var blobItems = await containerClient.GetBlobsAsync(prefix: containerPath).ToListAsync();

            return (containerName, blobItems);
        }
    }
}