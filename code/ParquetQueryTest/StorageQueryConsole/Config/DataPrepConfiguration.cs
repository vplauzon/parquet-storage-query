using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StorageQueryConsole.Config
{
    internal class DataPrepConfiguration
    {
        public Uri OriginDataFolderUri { get; set; } = new Uri("Undefined");

        public Uri DestinationDataFolderUri { get; set; } = new Uri("Undefined");
    }
}