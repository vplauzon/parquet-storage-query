using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Exceptions;
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
                if (config.DataPrep != null && config.DataPrep.Any())
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
            Console.WriteLine();
        }

        private static async Task<IImmutableList<PathMapping>> GetPathMappingsAsync(
            TokenCredential storageCredential,
            Uri originDataFolderUri,
            Uri destinationDataFolderUri,
            int? blobSizeTarget)
        {
            var blobs = await BlobCollection.LoadBlobsAsync(
                storageCredential,
                originDataFolderUri);

            if (blobSizeTarget == null)
            {
                var mappings = blobs
                    .BlobItems
                    .Select(i => new PathMapping(
                        new[] { $"{blobs.BlobContainerClient.Uri}/{i.Name}" },
                        $"{destinationDataFolderUri}/"
                        + $"{i.Name.Replace(".csv.gz", string.Empty)}"));

                return mappings.ToImmutableArray();
            }
            else
            {
                var mappingsBuilder = ImmutableArray<PathMapping>.Empty.ToBuilder();
                var currentOriginalPaths = new List<string>();
                var currentTotalBlobSize = (long)0;
                var currentCounter = 0;

                foreach (var item in blobs.BlobItems)
                {
                    if (currentTotalBlobSize + item.Properties.ContentLength
                        > blobSizeTarget * 1024 * 1024)
                    {
                        mappingsBuilder.Add(new PathMapping(
                            currentOriginalPaths,
                            $"{destinationDataFolderUri}/{currentCounter}"));
                        currentOriginalPaths.Clear();
                        currentTotalBlobSize = 0;
                        ++currentCounter;
                    }
                    currentOriginalPaths.Add($"{blobs.BlobContainerClient.Uri}/{item.Name}");
                    currentTotalBlobSize += item.Properties.ContentLength ?? 0;
                }

                if (currentOriginalPaths.Any())
                {
                    mappingsBuilder.Add(new PathMapping(
                        currentOriginalPaths,
                        $"{destinationDataFolderUri}/{currentCounter}"));
                }

                return mappingsBuilder.ToImmutableArray();
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

            await ExecuteControlCommandAsync(kustoCommandProvider, adxDatabase, commandText);
        }

        private static async Task ExecuteControlCommandAsync(
            ICslAdminProvider kustoCommandProvider,
            string adxDatabase,
            string commandText)
        {
            try
            {
                await kustoCommandProvider.ExecuteControlCommandAsync(
                    adxDatabase,
                    commandText);
            }
            catch (KustoException ex)
            {
                Console.WriteLine($"Exception in command exec:  {ex.Message}");

                if (!ex.IsPermanent)
                {
                    await ExecuteControlCommandAsync(
                        kustoCommandProvider,
                        adxDatabase,
                        commandText);
                }
            }
        }
    }
}