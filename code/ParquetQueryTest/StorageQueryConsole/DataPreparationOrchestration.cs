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
        #region Inner Types
        private class PathMapping
        {
            public PathMapping(IEnumerable<string> originalPaths, string destinationPath)
            {
                OriginalPaths = originalPaths.ToImmutableArray();
                DestinationPath = destinationPath;
            }

            public IImmutableList<string> OriginalPaths { get; }

            public string DestinationPath { get; }
        }
        #endregion

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
            var pathMappings = await GetPathMappingsAsync(
                storageCredential,
                dataPrepNode.OriginDataFolderUri,
                dataPrepNode.DestinationDataFolderUri,
                dataPrepNode.BlobSizeTarget);
            var blobCount = pathMappings.Sum(m => m.OriginalPaths.Count);

            Console.WriteLine($"From {dataPrepNode.OriginDataFolderUri}:  {blobCount} blobs");
            Console.WriteLine(
                $"To {dataPrepNode.DestinationDataFolderUri}:  {pathMappings.Count} blobs");

            foreach (var mapping in pathMappings)
            {
                await LoadCsvsIntoParquetAsync(
                    kustoCommandProvider,
                    adxDatabase,
                    mapping.OriginalPaths,
                    mapping.DestinationPath);
                Console.WriteLine($"Wrote '{mapping.DestinationPath}'");
            }
        }

        private static async Task<IImmutableList<PathMapping>> GetPathMappingsAsync(
            TokenCredential storageCredential,
            Uri originDataFolderUri,
            Uri destinationDataFolderUri,
            int? blobSizeTarget)
        {
            var originServiceUri = new Uri($"https://{originDataFolderUri.Host}");
            var pathParts = originDataFolderUri.LocalPath.Split('/').Skip(1);
            var originContainerName = pathParts.First();
            var containerPath = string.Join('/', pathParts.Skip(1));
            var blobClient = new BlobServiceClient(originServiceUri, storageCredential);
            var containerClient = blobClient.GetBlobContainerClient(originContainerName);
            var blobItems = await containerClient.GetBlobsAsync(prefix: containerPath).ToListAsync();

            if (blobSizeTarget == null)
            {
                var mappings = blobItems
                    .Select(i => new PathMapping(
                        new[] { $"{originServiceUri}/{originContainerName}/{i.Name}" },
                        $"{destinationDataFolderUri}/"
                        + $"{i.Name.Replace(".csv.gz", string.Empty)}"));

                return mappings.ToImmutableArray();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private static async Task LoadCsvsIntoParquetAsync(
            ICslAdminProvider kustoCommandProvider,
            string adxDatabase,
            IImmutableList<string> originalPaths,
            string destinationPath)
        {
            var originalPathsText =
                string.Join(", ", originalPaths.Select(p => $"h@'{p};impersonate'"));
            var commandText = @$".export compressed to parquet (
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
                      [{originalPathsText}]
                      with(format = 'csv')";

            await kustoCommandProvider.ExecuteControlCommandAsync(
                adxDatabase,
                commandText);
        }
    }
}