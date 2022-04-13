using Azure.Core;
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

        private static Task RunAsync(
            TokenCredential storageCredential,
            Uri adxClusterUri,
            string adxDatabase,
            QueryConfiguration queryNode)
        {
            throw new NotImplementedException();
        }
    }
}