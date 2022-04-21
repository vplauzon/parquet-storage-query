using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using StorageQueryConsole.Config;
using System.Collections.Immutable;
using System.Data;
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
                var builder = new KustoConnectionStringBuilder(adxClusterUri.ToString())
                    .WithAadUserPromptAuthentication();
                var commandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
                var queryProvider = KustoClientFactory.CreateCslQueryProvider(builder);

                //  From https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-query-acceleration-how-to
                switch (queryNode.QueryType)
                {
                    case QueryType.TotalCount:
                        await QueryTotalCountAsync(
                            storageCredential,
                            commandProvider,
                            queryProvider,
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
            ICslAdminProvider commandProvider,
            ICslQueryProvider queryProvider,
            string adxDatabase,
            Uri dataFolderUri)
        {
            await QueryBothSystemsAsync<CountResult>(
                storageCredential,
                commandProvider,
                queryProvider,
                adxDatabase,
                dataFolderUri,
                "SELECT COUNT(1) FROM BlobStorage",
                results =>
                {
                    var count = results.SelectMany(i => i).Select(r => r.Count).Sum();

                    Console.WriteLine($"Count by storage query:  {count}");
                },
                "summarize count()");
        }

        private static async Task QueryBothSystemsAsync<RESULT>(
            TokenCredential storageCredential,
            ICslAdminProvider commandProvider,
            ICslQueryProvider queryProvider,
            string adxDatabase,
            Uri dataFolderUri,
            string storageQueryText,
            Action<IEnumerable<IEnumerable<RESULT>>> showResultAction,
            string adxQueryText)
        {
            Console.WriteLine();
            Console.WriteLine($"Testing on storage {dataFolderUri}");
            Console.WriteLine($"Storage query:  {storageQueryText}");

            var storageResults = await QueryStorageAsync<RESULT>(
                storageCredential,
                dataFolderUri,
                storageQueryText);

            showResultAction(storageResults);

            Console.WriteLine("Testing on ADX");
            Console.WriteLine($"ADX query:  {adxQueryText}");

            await QueryAdxAsync(
                commandProvider,
                queryProvider,
                adxDatabase,
                dataFolderUri,
                adxQueryText);
        }

        private static async Task QueryAdxAsync(
            ICslAdminProvider commandProvider,
            ICslQueryProvider queryProvider,
            string adxDatabase,
            Uri dataFolderUri,
            string queryText)
        {
            var watch = new Stopwatch();
            var externalTableName = "ParquetTable";

            await commandProvider.ExecuteControlCommandAsync(
                adxDatabase,
                $".drop external table {externalTableName} ifexists");
            await commandProvider.ExecuteControlCommandAsync(
                adxDatabase,
                @$".create external table {externalTableName}
(Timestamp:datetime, Instance:string, Node:string, Level:string, Component:string, EventId:string, Detail:string)
kind=storage 
dataformat=parquet
( 
   h@'{dataFolderUri};impersonate' 
)");
            watch.Start();
            await queryProvider.ExecuteQueryAsync(
                adxDatabase,
                $"external_table('{externalTableName}') | {queryText}",
                new ClientRequestProperties());
            Console.WriteLine($"Query cold:  {watch.Elapsed}");
            watch.Restart();
            var result = await queryProvider.ExecuteQueryAsync(
                adxDatabase,
                $"external_table('{externalTableName}') | {queryText}",
                new ClientRequestProperties());
            var table = new DataTable();

            table.Load(result);
            Console.WriteLine($"Query warm:  {watch.Elapsed}");

            foreach (var row in table.Rows.Cast<DataRow>())
            {
                Console.WriteLine(string.Join(' ', row.ItemArray));
            }
        }

        private static async Task<IEnumerable<IEnumerable<RESULT>>> QueryStorageAsync<RESULT>(
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

            Console.WriteLine($"# of blobs:  {nonEmptyBlobClients.Count()}");
            Console.WriteLine($"Blob retrieval:  {watch.Elapsed}");
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
            Console.WriteLine($"Query:  {watch.Elapsed}");

            var values = queryTasks.Select(t => (IEnumerable<RESULT>)t.Result).ToImmutableArray();

            return values;
        }
    }
}