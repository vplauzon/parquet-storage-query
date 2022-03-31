using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StorageQueryConsole.Config
{
    internal class RootConfiguration
    {
        public AuthenticationMode AuthenticationMode { get; set; } = AuthenticationMode.AzCli;
        
        public Uri? AdxClusterUri{get;set;}
        
        public string? AdxDatabase { get;set;}

        public IImmutableList<DataPrepConfiguration> DataPrep { get; set; }
            = ImmutableArray<DataPrepConfiguration>.Empty;
    }
}