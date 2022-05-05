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
            //ServicePointManager.SetTcpKeepAlive(false, 0, 0);

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
                    config.AuthenticationMode,
                    config.AppDetails);
                var kustoCommandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
                var kustoQueryProvider = KustoClientFactory.CreateCslQueryProvider(builder);
                var storageCredential = GetStorageCredentials(config.AuthenticationMode, config.AppDetails);

                await DataPreparationOrchestration.RunAsync(kustoCommandProvider, storageCredential, config);
                await QueryOrchestration.RunAsync(
                    kustoCommandProvider,
                    kustoQueryProvider,
                    storageCredential,
                    config);
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
            AuthenticationMode authenticationMode,
            AppDetail appDetails)
        {
            var builder = new KustoConnectionStringBuilder(adxClusterUri.ToString());

            switch (authenticationMode)
            {
                case AuthenticationMode.AzCli:
                    return builder.WithAadAzCliAuthentication();
                case AuthenticationMode.Browser:
                    return builder.WithAadUserPromptAuthentication();
                case AuthenticationMode.AppSecret:
                    return builder.WithAadApplicationKeyAuthentication(
                        appDetails.AppId,
                        appDetails.Secret,
                        appDetails.TenantId);

                default:
                    throw new NotSupportedException(
                        $"Authentication mode not supported:  '{authenticationMode}'");
            }
        }

        private static TokenCredential GetStorageCredentials(AuthenticationMode authenticationMode, AppDetail appDetails)
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
                case AuthenticationMode.AppSecret:
                    return new ClientSecretCredential(
                        appDetails.TenantId,
                        appDetails.AppId,
                        appDetails.Secret);

                default:
                    throw new NotSupportedException(
                        $"Authentication mode not supported:  '{authenticationMode}'");
            }
        }
    }
}