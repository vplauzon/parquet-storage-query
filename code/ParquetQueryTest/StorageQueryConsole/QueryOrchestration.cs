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
        private class Log
        {
            [Index(0)]
            public DateTime Timestamp { get; set; }

            [Index(1)]
            public string Instance { get; set; } = "";

            [Index(2)]
            public string Node { get; set; } = "";

            [Index(3)]
            public string Level { get; set; } = "";

            [Index(4)]
            public string Component { get; set; } = "";

            [Index(5)]
            public string EventId { get; set; } = "";

            [Index(6)]
            public string Detail { get; set; } = "";
        }

        private class CountResult
        {
            [Index(0)]
            public long Count { get; set; }
        }

        private class MinMaxResult<T>
            where T : struct
        {
            [Index(0)]
            public T Min { get; set; }

            [Index(1)]
            public T Max { get; set; }
        }

        private class MaxByResult
        {
            [Index(0)]
            public DateTime Max { get; set; }

            [Index(1)]
            public string? By { get; set; }
        }

        private class CountByResult
        {
            [Index(0)]
            public long Count { get; set; }

            [Index(1)]
            public string? By { get; set; }
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
                    case QueryType.TimeFilterCount:
                        await QueryTimeFilterCountAsync(
                            storageCredential,
                            commandProvider,
                            queryProvider,
                            adxDatabase,
                            queryNode.DataFolderUri);

                        return;
                    case QueryType.FilterCount:
                        await QueryFilterCountAsync(
                            storageCredential,
                            commandProvider,
                            queryProvider,
                            adxDatabase,
                            queryNode.DataFolderUri);

                        return;
                    case QueryType.MinMax:
                        await QueryMinMaxAsync(
                            storageCredential,
                            commandProvider,
                            queryProvider,
                            adxDatabase,
                            queryNode.DataFolderUri);

                        return;
                    case QueryType.MaxBy:
                        await QueryMaxByAsync(
                            storageCredential,
                            commandProvider,
                            queryProvider,
                            adxDatabase,
                            queryNode.DataFolderUri);

                        return;
                    case QueryType.PointFilter:
                        await QueryPointFilterAsync(
                            storageCredential,
                            commandProvider,
                            queryProvider,
                            adxDatabase,
                            queryNode.DataFolderUri);

                        return;
                    case QueryType.Distinct:
                        await QueryDistinctAsync(
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

        private static async Task QueryDistinctAsync(
            TokenCredential storageCredential,
            ICslAdminProvider commandProvider,
            ICslQueryProvider queryProvider,
            string adxDatabase,
            Uri dataFolderUri)
        {
            await QueryBothSystemsAsync<CountByResult>(
                storageCredential,
                commandProvider,
                queryProvider,
                adxDatabase,
                dataFolderUri,
                "SELECT 'Instance', COUNT(*) FROM BlobStorage GROUP BY 'Instance'",
                results =>
                {
                    var distinctValues = results
                    .SelectMany(i => i)
                    .Select(r => r.By)
                    .Distinct();

                    Console.WriteLine("Distinct Instance by storage query:");
                    foreach (var e in distinctValues)
                    {
                        Console.WriteLine(e);
                    }
                },
                "distinct Instance");
        }

        private static async Task QueryPointFilterAsync(
            TokenCredential storageCredential,
            ICslAdminProvider commandProvider,
            ICslQueryProvider queryProvider,
            string adxDatabase,
            Uri dataFolderUri)
        {
            await QueryBothSystemsAsync<Log>(
                storageCredential,
                commandProvider,
                queryProvider,
                adxDatabase,
                dataFolderUri,
                //"SELECT * FROM BlobStorage WHERE _6=\"c2275e14-2311-9296-894f-9567a0426fcc\"",
                "SELECT * FROM BlobStorage WHERE 'EventId'='c2275e14-2311-9296-894f-9567a0426fcc'",
                results =>
                {
                    var rows = results.SelectMany(i => i);

                    Console.WriteLine($"Point filter by storage query:  {rows.Count()} results");
                    foreach (var r in rows)
                    {
                        Console.WriteLine($"{r.Timestamp} {r.Instance} {r.Node} {r.Level} {r.Component} {r.EventId} {r.Detail}");
                    }
                },
                "where EventId=='c2275e14-2311-9296-894f-9567a0426fcc'");
        }

        private static async Task QueryMaxByAsync(
            TokenCredential storageCredential,
            ICslAdminProvider commandProvider,
            ICslQueryProvider queryProvider,
            string adxDatabase,
            Uri dataFolderUri)
        {
            await QueryBothSystemsAsync<MaxByResult>(
                storageCredential,
                commandProvider,
                queryProvider,
                adxDatabase,
                dataFolderUri,
                "SELECT Max('Timestamp'), 'Level' FROM BlobStorage GROUP BY 'Level'",
                results =>
                {
                    var maxByValues = results
                    .SelectMany(i => i)
                    .GroupBy(r => r.By)
                    .Select(g => new MaxByResult
                    {
                        Max = g.Max(e => e.Max),
                        By = g.Key
                    });

                    Console.WriteLine("Max Timestamp by Level by storage query:");
                    foreach (var e in maxByValues)
                    {
                        Console.WriteLine($"{e.Max} / {e.By}");
                    }
                },
                "summarize max(Timestamp) by Level");
        }

        private static async Task QueryMinMaxAsync(
            TokenCredential storageCredential,
            ICslAdminProvider commandProvider,
            ICslQueryProvider queryProvider,
            string adxDatabase,
            Uri dataFolderUri)
        {
            await QueryBothSystemsAsync<MinMaxResult<DateTime>>(
                storageCredential,
                commandProvider,
                queryProvider,
                adxDatabase,
                dataFolderUri,
                "SELECT Min('Timestamp'), Max('Timestamp') FROM BlobStorage",
                results =>
                {
                    var minValue = results.SelectMany(i => i).Min(r => r.Min);
                    var maxValue = results.SelectMany(i => i).Max(r => r.Max);

                    Console.WriteLine($"Min-Max Timestamp by storage query:  {minValue} / {maxValue}");
                },
                "summarize min(Timestamp), max(Timestamp)");
        }

        private static async Task QueryFilterCountAsync(
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
                "SELECT COUNT(*) FROM BlobStorage WHERE 'Level'='Warning'",
                results =>
                {
                    var count = results.SelectMany(i => i).Select(r => r.Count).Sum();

                    Console.WriteLine($"Count by storage query:  {count}");
                },
                "where Level=='Warning'| count");
        }

        private static async Task QueryTimeFilterCountAsync(
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
                "SELECT COUNT(1) FROM BlobStorage WHERE 'Timestamp'>TO_TIMESTAMP('2014-03-08T04:00:00.00Z')",
                results =>
                {
                    var count = results.SelectMany(i => i).Select(r => r.Count).Sum();

                    Console.WriteLine($"Count by storage query:  {count}");
                },
                "where Timestamp>datetime(2014-03-08T04:00:00)| count");
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
                "count");
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