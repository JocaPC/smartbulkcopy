using System;
using System.IO;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;

namespace SmartBulkCopy
{
    enum SafeCheck {
        None,
        Snapshot,
        ReadOnly
    }

    class SmartBulkCopyConfiguration 
    {
        public enum CopyType { BulkCopy, ServerSidePush, ServerSidePull }
        internal CopyType type;

        public void SetType (string value) {
            switch (value.ToLower()) {
                case "serverside":
                case "serversidepush":
                    this.type = CopyType.ServerSidePush;
                    break;
                case "serversidepull":
                    this.type = CopyType.ServerSidePull;
                    break;
                case "bulkcopy":
                case "bulk_copy":
                default:
                    this.type = CopyType.BulkCopy;
                    break;                
            }
        }

        public string SourceConnectionString;

        public string SourceDatabase { get; private set; }

        public string DestinationConnectionString;

        string _destinationDatabaseName;
        public string DestinationDatabase
        {   get => _destinationDatabaseName;
            private set {
                if (string.IsNullOrWhiteSpace(value) && type == CopyType.ServerSidePush)
                    throw new InvalidOperationException("Destination database name must be set in ServerSidePush mode");
                else _destinationDatabaseName = value;
             }
        }

        public string LinkedServer { get; private set; }

        public List<string> TablesToCopy = new List<string>();

        private int _batchSize = 100000;
        public int BatchSize {
            get { return _batchSize; }
            set {
                if (value < 1000) throw new ArgumentException($"{nameof(BatchSize)}cannot be less than 1000");
                if (value > 100000000) throw new ArgumentException($"{nameof(BatchSize)} cannot be greather than 100000000");
                _batchSize = value;
            }
        }

        private int _maxParallelTasks = 7;
        public int MaxParallelTasks {
            get {
                return _maxParallelTasks;
            }
            set {
                if (value < 1) throw new ArgumentException($"{nameof(MaxParallelTasks)}cannot be less than 1");
                if (value > 32) throw new ArgumentException($"{nameof(MaxParallelTasks)} cannot be greather than 32");
                _maxParallelTasks = value;
            }
        }

        private int _logicalPartitions = 7;
        public int LogicalPartitions  {
            get {
                return _logicalPartitions;
            }
            set {
                if (value < 1) throw new ArgumentException($"{nameof(LogicalPartitions)} cannot be less than 1");
                if (value > 32) throw new ArgumentException($"{nameof(LogicalPartitions)} cannot be greather than 32");
                _logicalPartitions = value;
            }
        }

        public bool TruncateTables = false;

        public SafeCheck SafeCheck = SafeCheck.ReadOnly;

        private SmartBulkCopyConfiguration() {}

        public static SmartBulkCopyConfiguration LoadFromConfigFile()
        {
            return LoadFromConfigFile("smartbulkcopy.config");
        }

        public static SmartBulkCopyConfiguration LoadFromConfigFile(string configFile)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile(configFile, optional: false, reloadOnChange: false)
                .Build();                 

            var sbcc = new SmartBulkCopyConfiguration();

            sbcc.SetType(config?["type"]??"bulkcopy");
            sbcc.SourceConnectionString = config["source:connection-string"];
            sbcc.DestinationConnectionString = config["destination:connection-string"];
            sbcc.DestinationDatabase = config["destination:destination-database"];
            sbcc.LinkedServer = config["destination:linked-server"] ?? "DESTINATION";
            sbcc.BatchSize = int.Parse(config?["options:batch-size"] ?? sbcc.BatchSize.ToString());
            sbcc.LogicalPartitions = int.Parse(config?["options:logical-partitions"] ?? sbcc.LogicalPartitions.ToString());
            sbcc.MaxParallelTasks = int.Parse(config?["options:tasks"] ?? sbcc.MaxParallelTasks.ToString());
            sbcc.TruncateTables = bool.Parse(config?["options:truncate-tables"] ?? sbcc.TruncateTables.ToString());
            
            var safeCheck = config?["options:safe-check"];
            if (!string.IsNullOrEmpty(safeCheck))
            {
                switch (safeCheck.ToLower()) 
                {
                    case "none": sbcc.SafeCheck = SafeCheck.None;
                        break;

                    case "read-only":
                    case "readonly": sbcc.SafeCheck = SafeCheck.ReadOnly;
                        break;

                    case "snapshot": sbcc.SafeCheck = SafeCheck.Snapshot;
                        break;

                    default: 
                        throw new ArgumentException("Option safe-check can only contain 'none', 'readonly' or 'snapshot' values.");
                }
            }
            
            var tablesArray = config.GetSection("tables").GetChildren();                        
            foreach(var t in tablesArray) {
                sbcc.TablesToCopy.Add(t.Value);
            }

            return sbcc;
        }
    }
}