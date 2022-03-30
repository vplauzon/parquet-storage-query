using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using StorageQueryConsole.Config;
using System.Collections.Immutable;

namespace StorageQueryConsole
{
    internal class SameSizeCopyOrchestration
    {
        internal static async Task RunAsync(RootConfiguration config)
        {
            if (config.OriginDataFolder != null)
            {
                await RunAsync(config.AuthenticationMode, config.OriginDataFolder);
            }
        }

        private static async Task RunAsync(
            AuthenticationMode authenticationMode,
            string originDataFolder)
        {
            Console.WriteLine("Copy 'same size'");

            var blobItems = await GetBlobItemsAsync(authenticationMode, originDataFolder);

            Console.WriteLine($"{blobItems.Count} blobs");

            throw new NotImplementedException();
        }

        private static async Task<IImmutableList<BlobItem>> GetBlobItemsAsync(
            AuthenticationMode authenticationMode,
            string originDataFolder)
        {
            var originUri = new Uri(originDataFolder);
            var serviceUri = new Uri($"https://{originUri.Host}");
            var pathParts = originUri.LocalPath.Split('/').Skip(1);
            var containerName = pathParts.First();
            var containerPath = string.Join('/', pathParts.Skip(1));
            var credential = GetCredentials(authenticationMode);
            var blobClient = new BlobServiceClient(serviceUri, credential);
            var containerClient = blobClient.GetBlobContainerClient(containerName);
            var blobItems = await containerClient.GetBlobsAsync(prefix: containerPath).ToListAsync();

            return blobItems;
        }

        private static TokenCredential GetCredentials(AuthenticationMode authenticationMode)
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