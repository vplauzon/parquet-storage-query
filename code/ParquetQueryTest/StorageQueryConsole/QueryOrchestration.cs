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

                    foreach (var query in config.Queries)
                    {
                    }
                }
            }
        }
    }
}