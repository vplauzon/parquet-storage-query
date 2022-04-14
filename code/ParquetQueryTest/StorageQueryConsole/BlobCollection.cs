using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StorageQueryConsole
{
    internal class BlobCollection
    {
        public static async Task<BlobCollection> LoadBlobsAsync(
            TokenCredential storageCredential,
            Uri dataFolderUri)
        {
            var serviceUri = new Uri($"https://{dataFolderUri.Host}");
            var pathParts = dataFolderUri.LocalPath.Split('/').Skip(1);
            var containerName = pathParts.First();
            var containerPath = string.Join('/', pathParts.Skip(1));
            var blobClient = new BlobServiceClient(serviceUri, storageCredential);
            var containerClient = blobClient.GetBlobContainerClient(containerName);
            var blobItems = await containerClient.GetBlobsAsync(prefix: containerPath).ToListAsync();

            return new BlobCollection(
                blobClient.GetBlobContainerClient(containerName),
                blobItems);
        }

        private BlobCollection(
            BlobContainerClient blobContainerClient,
            IImmutableList<BlobItem> blobItems)
        {
            BlobContainerClient = blobContainerClient;
            BlobItems = blobItems;
        }

        public BlobContainerClient BlobContainerClient { get; }

        public IImmutableList<BlobItem> BlobItems { get; }
    }
}