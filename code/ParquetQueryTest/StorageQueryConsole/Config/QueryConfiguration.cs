using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StorageQueryConsole.Config
{
    internal class QueryConfiguration
    {
        public Uri DataFolderUri { get; set; } = new Uri("https://Undefined");

        public QueryType QueryType { get; set; } = QueryType.TotalCount;
    }
}