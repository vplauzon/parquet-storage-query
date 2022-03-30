using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StorageQueryConsole.Config
{
    internal class RootConfiguration
    {
        public AuthenticationMode AuthenticationMode { get; set; } = AuthenticationMode.AzCli;
        
        public string? OriginDataFolder { get; set; }
    }
}