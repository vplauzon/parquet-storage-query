using StorageQueryConsole.Config;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace StorageQueryConsole
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            //ServicePointManager.DefaultConnectionLimit = 25;

            if (args.Length < 1)
            {
                Console.Error.WriteLine("You have to call this exec with a parameter pointing to the config file");
            }
            else
            {
                var config = await LoadConfigAsync(args);
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
    }
}