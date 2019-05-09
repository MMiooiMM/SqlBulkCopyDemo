using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace SqlBulkCopyDemo
{
    public class SqlBulkCopyDemo
    {
        public string SourceConnectionString { get; set; }
        public string DestinationConnectionString { get; set; }
        public string SourceTableName { get; set; }
        public string DestinationTableName { get; set; }

        public SqlBulkCopyDemo(string sourceConnectionString, string destinationConnectionString, string sourceTableName, string destinationTableName)
        {
            SourceConnectionString = sourceConnectionString;
            DestinationConnectionString = destinationConnectionString;
            SourceTableName = sourceTableName;
            DestinationTableName = destinationTableName;
        }
        public void Copy()
        {
            using (SqlConnection sourceConnection = new SqlConnection(SourceConnectionString))
            {
                sourceConnection.Open();

                Print("CopyTable start");

                SqlCommand commandSourceData = new SqlCommand($"SELECT * FROM {SourceTableName};", sourceConnection);
                SqlDataReader reader = commandSourceData.ExecuteReader();
                
                using (SqlConnection destinationConnection = new SqlConnection(DestinationConnectionString))
                {
                    destinationConnection.Open();

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection))
                    {
                        bulkCopy.BatchSize = 100000; // 一次批次寫入多少筆資料
                        bulkCopy.NotifyAfter = 100000; // 在寫入多少筆資料後呼叫 SqlRowsCopied 事件
                        bulkCopy.SqlRowsCopied += OnSqlRowsCopied;
                        bulkCopy.BulkCopyTimeout = 1800; // 逾時秒數
                        bulkCopy.DestinationTableName = DestinationTableName; // 要寫入的資料表名稱
                        try
                        {
                            bulkCopy.WriteToServer(reader);
                        }
                        catch (Exception ex)
                        {
                            Print(ex.Message);
                        }
                        finally
                        {
                            reader.Close();
                            Print("CopyTable finish");
                        }
                    }                    
                }
            }
        }

        public void Copy(bool truncate, int batchSize, int timeout)
        {
            using (SqlConnection sourceConnection = new SqlConnection(SourceConnectionString))
            {
                sourceConnection.Open();

                Print("CopyTable start");

                SqlCommand commandSourceData = new SqlCommand($"SELECT * FROM {SourceTableName};", sourceConnection);
                SqlDataReader reader = commandSourceData.ExecuteReader();

                using (SqlConnection destinationConnection = new SqlConnection(DestinationConnectionString))
                {
                    destinationConnection.Open();

                    if (truncate)
                    {
                        Print($"truncate table {DestinationTableName}");
                        SqlCommand truncateDestinationData = new SqlCommand($"truncate table {DestinationTableName}", destinationConnection);
                        truncateDestinationData.ExecuteNonQuery();
                    }

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection))
                    {
                        bulkCopy.BatchSize = batchSize; // 一次批次寫入多少筆資料
                        bulkCopy.NotifyAfter = batchSize; // 在寫入多少筆資料後呼叫 SqlRowsCopied 事件
                        bulkCopy.SqlRowsCopied += OnSqlRowsCopied;
                        bulkCopy.BulkCopyTimeout = timeout; // 逾時秒數
                        bulkCopy.DestinationTableName = DestinationTableName; // 要寫入的資料表名稱
                        try
                        {
                            bulkCopy.WriteToServer(reader);
                        }
                        catch (Exception ex)
                        {
                            Print(ex.Message);
                        }
                        finally
                        {
                            reader.Close();
                            Print("CopyTable finish");
                        }
                    }
                }
            }
        }

        public void Copy(bool truncate, int batchSize, int timeout, string sourceColumns, string destinationColumns)
        {
            var sourceColumnArray = sourceColumns.Split(';');
            var destinationColumnArray = destinationColumns.Split(';');
            if(sourceColumnArray.Count() != destinationColumnArray.Count())
            {
                Print("sourceColumns not equal to destinationColumns");
                return;
            }
            using (SqlConnection sourceConnection = new SqlConnection(SourceConnectionString))
            {
                sourceConnection.Open();

                Print("CopyTable start");

                SqlCommand commandSourceData = new SqlCommand($"SELECT {string.Join(",", sourceColumnArray)} FROM {SourceTableName};", sourceConnection);
                SqlDataReader reader = commandSourceData.ExecuteReader();

                using (SqlConnection destinationConnection = new SqlConnection(DestinationConnectionString))
                {
                    destinationConnection.Open();

                    if (truncate)
                    {
                        Print($"truncate table {DestinationTableName}");
                        SqlCommand truncateDestinationData = new SqlCommand($"truncate table {DestinationTableName}", destinationConnection);
                        truncateDestinationData.ExecuteNonQuery();
                    }

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection))
                    {
                        bulkCopy.BatchSize = batchSize; // 一次批次寫入多少筆資料
                        bulkCopy.NotifyAfter = batchSize; // 在寫入多少筆資料後呼叫 SqlRowsCopied 事件
                        bulkCopy.SqlRowsCopied += OnSqlRowsCopied;
                        bulkCopy.BulkCopyTimeout = timeout; // 逾時秒數
                        bulkCopy.DestinationTableName = DestinationTableName; // 要寫入的資料表名稱
                        for(int i=0;i< sourceColumnArray.Count(); i++)
                        {
                            bulkCopy.ColumnMappings.Add(sourceColumnArray[i], destinationColumnArray[i]);
                        }
                        try
                        {
                            bulkCopy.WriteToServer(reader);
                        }
                        catch (Exception ex)
                        {
                            Print(ex.Message);
                        }
                        finally
                        {
                            reader.Close();
                            Print("CopyTable finish");
                        }
                    }
                }
            }
        }

        private void OnSqlRowsCopied(Object sender, SqlRowsCopiedEventArgs args)
        {
            Print($"{args.RowsCopied.ToString()} rows are copied.");
        }

        private void Print(string message)
        {
            Console.WriteLine($"{DateTime.Now.ToString("yyyy/MM/dd hh:mm:ss")} {message}");
        }
    }
}
