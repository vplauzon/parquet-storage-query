using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Kusto.Data.Common;
using StorageQueryConsole.Config;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;

namespace StorageQueryConsole
{
    internal class QueryOrchestration
    {
        #region Inner Types
        private class CountResult
        {
            [Index(0)]
            public long Count { get; set; }
        }
        #endregion

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
            var counts = await QueryAsync<CountResult>(
                storageCredential,
                dataFolderUri,
                "SELECT COUNT(1) FROM BlobStorage");
            var count = counts.SelectMany(i => i).Select(r => r.Count).Sum();

            Console.WriteLine($"Count by storage query:  {count}");
        }

        private static async Task<IEnumerable<IEnumerable<RESULT>>> QueryAsync<RESULT>(
            TokenCredential storageCredential,
            Uri dataFolderUri,
            string queryText)
        {
            var watch = new Stopwatch();

            watch.Start();

            var blobs = await BlobCollection.LoadBlobsAsync(
                storageCredential,
                dataFolderUri);
            var nonEmptyBlobClients = blobs
                .BlobItems
                .Where(i => i.Properties.ContentLength != 0)
                .Select(i => blobs.BlobContainerClient.GetBlockBlobClient(i.Name));
            var options = new BlobQueryOptions
            {
                InputTextConfiguration = new BlobQueryParquetTextOptions()
            };
            var elapsedRetrieveBlobs = watch.Elapsed;

            options.ErrorHandler += (BlobQueryError err) =>
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Error.WriteLine($"Error: {err.Position}:{err.Name}:{err.Description}");
                Console.ResetColor();
            };
            watch.Restart();

            var queryTasks = nonEmptyBlobClients.Select(async b =>
            {
                using (var streamReader = new StreamReader((await b.QueryAsync(
                   queryText,
                   options)).Value.Content))
                using (var csvReader = new CsvReader(
                    streamReader,
                    new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = false }))
                {
                    var counts = csvReader.GetRecords<RESULT>().ToImmutableArray();

                    return counts;
                }
            }).ToImmutableArray();

            await Task.WhenAll(queryTasks);

            var elapsedQuery = watch.Elapsed;
            var values = queryTasks.Select(t => (IEnumerable<RESULT>)t.Result).ToImmutableArray();

            Console.WriteLine($"Blob retrieval:  {elapsedRetrieveBlobs}");
            Console.WriteLine($"Query:  {elapsedQuery}");
            Console.WriteLine($"# of blobs:  {nonEmptyBlobClients.Count()}");

            return values;
        }
    }
}