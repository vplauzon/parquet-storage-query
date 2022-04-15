using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Kusto.Data.Common;
using StorageQueryConsole.Config;

namespace StorageQueryConsole
{
    internal class QueryOrchestration
    {
        internal static async Task RunAsync(
            ICslAdminProvider kustoCommandProvider,
            TokenCredential storageCredential,
            RootConfiguration config)
        {
            if (config.AdxClusterUri != null && config.AdxDatabase != null)
            {
                if (config.Queries != null && config.Queries.Any())
                {
                    Console.WriteLine("Queries...");
                    Console.WriteLine();

                    foreach (var queryNode in config.Queries)
                    {
                        await RunAsync(
                            storageCredential,
                            config.AdxClusterUri,
                            config.AdxDatabase,
                            queryNode);
                    }
                }
            }
        }

        private static async Task RunAsync(
            TokenCredential storageCredential,
            Uri adxClusterUri,
            string adxDatabase,
            QueryConfiguration queryNode)
        {
            if (queryNode.DataFolderUri != null)
            {
                //  From https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-query-acceleration-how-to
                switch (queryNode.QueryType)
                {
                    case QueryType.TotalCount:
                        await QueryTotalCountAsync(
                            storageCredential,
                            adxClusterUri,
                            adxDatabase,
                            queryNode.DataFolderUri);

                        return;
                    default:
                        throw new NotSupportedException($"Query type '{queryNode.QueryType}'");
                }
            }
        }

        private static async Task QueryTotalCountAsync(
            TokenCredential storageCredential,
            Uri adxClusterUri,
            String adxDatabase,
            Uri dataFolderUri)
        {
            var blobs = await BlobCollection.LoadBlobsAsync(
                storageCredential,
                dataFolderUri);
            var nonEmptyBlobClients = blobs
                .BlobItems
                .Where(i => i.Properties.ContentLength != 0)
                .Select(i => blobs.BlobContainerClient.GetBlockBlobClient(i.Name));
            var firstBlobClient = nonEmptyBlobClients.First();
            var options = new BlobQueryOptions
            {
                InputTextConfiguration = new BlobQueryParquetTextOptions(),
                OutputTextConfiguration = new BlobQueryParquetTextOptions()
            };

            options.ErrorHandler += (BlobQueryError err) =>
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Error.WriteLine($"Error: {err.Position}:{err.Name}:{err.Description}");
                Console.ResetColor();
            };

            using (var reader = new StreamReader((await firstBlobClient.QueryAsync(
               "SELECT COUNT(1) FROM BlobStorage",
               options)).Value.Content))
            {
                var text = await reader.ReadToEndAsync();
            }

            throw new NotImplementedException();
        }
    }
}