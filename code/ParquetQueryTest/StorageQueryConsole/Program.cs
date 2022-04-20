using Azure.Core;
using Azure.Identity;
using Kusto.Data;
using Kusto.Data.Net.Client;
using StorageQueryConsole.Config;
using System.Net;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace StorageQueryConsole
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = 2000;

            if (args.Length < 1)
            {
                Console.Error.WriteLine(
                    "You have to call this exec with a parameter pointing to the config file");
            }
            else
            {
                var config = await LoadConfigAsync(args);
                var builder = GetKustoConnectionStringBuilder(
                    config.AdxClusterUri,
                    config.AuthenticationMode);
                var kustoCommandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
                var storageCredential = GetStorageCredentials(config.AuthenticationMode);

                await DataPreparationOrchestration.RunAsync(kustoCommandProvider, storageCredential, config);
                await QueryOrchestration.RunAsync(kustoCommandProvider, storageCredential, config);
            }
        }

        private static async Task<RootConfiguration> LoadConfigAsync(string[] args)
        {
            var configPath = args[0];
            var configContent = await File.ReadAllTextAsync(configPath);
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();
            var config = deserializer.Deserialize<RootConfiguration>(configContent);

            return config;
        }

        private static KustoConnectionStringBuilder GetKustoConnectionStringBuilder(
            Uri adxClusterUri,
            AuthenticationMode authenticationMode)
        {
            var builder = new KustoConnectionStringBuilder(adxClusterUri.ToString());

            switch (authenticationMode)
            {
                case AuthenticationMode.AzCli:
                    return builder.WithAadAzCliAuthentication();
                case AuthenticationMode.Browser:
                    return builder.WithAadUserPromptAuthentication();

                default:
                    throw new NotSupportedException(
                        $"Authentication mode not supported:  '{authenticationMode}'");
            }
        }

        private static TokenCredential GetStorageCredentials(AuthenticationMode authenticationMode)
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