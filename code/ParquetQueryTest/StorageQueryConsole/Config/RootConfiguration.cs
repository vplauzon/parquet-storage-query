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

        public Uri AdxClusterUri { get; set; } = new Uri("https://Undefined");

        public string AdxDatabase { get; set; } = "No DB Defined";

        public DataPrepConfiguration[] DataPrep { get; set; } = new DataPrepConfiguration[0];
        
        public QueryConfiguration[] Queries { get; set; } = new QueryConfiguration[0];
    }
}