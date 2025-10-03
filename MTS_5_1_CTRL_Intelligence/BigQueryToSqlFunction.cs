using Google.Cloud.BigQuery.V2;
using Google.Cloud.Functions.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;

namespace MTS_5_1_CTRL_Intelligence
{
    public class BigQueryToSqlFunction : IHttpFunction
    {
        private readonly ILogger _logger;
        private readonly string _projectId;
        private readonly string _datasetId;
        public BigQueryToSqlFunction(ILogger<Function> logger)
        {
            _logger = logger;
            _projectId = Environment.GetEnvironmentVariable("GCP_PROJECT") ?? "";
            _datasetId = Environment.GetEnvironmentVariable("DATASET_ID");
        }

        public async Task HandleAsync(HttpContext context)
        {
            _logger.LogInformation("Function started.");

            string ns1Conn = Environment.GetEnvironmentVariable("KF_CONSTRING");
            string ns2Conn = Environment.GetEnvironmentVariable("PI_CONSTRING");


            _logger.LogInformation("Using projectId: {projectId}, ns1Conn: {ns1Conn}, ns2Conn: {ns2Conn}",
                _projectId, ns1Conn, ns2Conn);

            if (string.IsNullOrEmpty(_projectId) || string.IsNullOrEmpty(ns1Conn) || string.IsNullOrEmpty(ns2Conn))
            {
                _logger.LogError("Environment variables not configured properly.");
                context.Response.StatusCode = 500;
                await context.Response.WriteAsync("ERROR: Missing environment variables.");
                return;
            }

            try
            {
                BigQueryClient client = BigQueryClient.Create(_projectId);

                // Fetch only today's rows in UTC by formatting DateProcessed
                string dateFilter = DateTime.UtcNow.ToString("yyyy-MM-dd");

                string query = $@"
                    SELECT * 
                    FROM `{_projectId}.{_datasetId}.file_processed_table`
                    WHERE DATE(DateProcessed) = '{dateFilter}'";

                _logger.LogInformation("Executing query: {query}", query);

                BigQueryResults result = client.ExecuteQuery(query, parameters: null);

                // Prepare DataTables for batch insert
                var ns1Table = CreateDataTable();
                var ns2Table = CreateDataTable();

                _logger.LogInformation("Processing rows from BigQuery.", result.GetRowsAsync());

                await foreach (var row in result.GetRowsAsync())
                {
                    _logger.LogInformation("Processing row: {row}", row);
                    var fileFormat = row["FileFormat"]?.ToString();



                    _logger.LogInformation("Processing row with FileFormat: {fileFormat}", fileFormat);

                    try
                    {
                        if (fileFormat == "NS1")
                        {
                            DataRow dataRow = ns1Table.NewRow();
                            FillDataRow(row, dataRow);
                            ns1Table.Rows.Add(dataRow);
                        }
                        else if (fileFormat == "NS2")
                        {
                            DataRow dataRow = ns2Table.NewRow();
                            FillDataRow(row, dataRow);
                            ns2Table.Rows.Add(dataRow);
                        }
                        else
                        {
                            _logger.LogWarning("Skipping unsupported FileFormat: {fileFormat}", fileFormat);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Log the specific row details causing the error
                        _logger.LogError(ex, "Failed to fill DataRow for FileFormat {FileFormat}. BigQuery row keys: {Keys}",
                            fileFormat, string.Join(", ", row.Schema.Fields.Select(f => f.Name)));

                        // Skip this problematic row instead of letting the entire function fail
                        continue;
                    }

                    _logger.LogInformation("Added row to {CardholderNumber} table.", row["CardholderNumber"]?.ToString());
                }

                int totalInserted = 0;

                _logger.LogInformation("ns1Table count: ", ns1Table.Rows, ns1Table.Rows.Count);


                // Batch insert NS1 rows
                if (ns1Table.Rows.Count > 0)
                {
                    totalInserted += await BulkInsert(ns1Conn, ns1Table, "trans_kf");
                }

                _logger.LogInformation("ns2Table count: ", ns2Table.Rows, ns2Table.Rows.Count);

                // Batch insert NS2 rows
                if (ns2Table.Rows.Count > 0)
                {
                    totalInserted += await BulkInsert(ns2Conn, ns2Table, "trans_pi");
                }

                _logger.LogInformation("Total rows inserted: {totalInserted}", totalInserted);
                await context.Response.WriteAsync($"Successfully inserted {totalInserted} rows into target databases.");
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error processing BigQuery data.");
                context.Response.StatusCode = 500;
                await context.Response.WriteAsync("ERROR: Could not process data.");
            }
        }

        private DataTable CreateDataTable()
        {
            var dt = new DataTable();
            dt.Columns.Add("FileName", typeof(string));
            dt.Columns.Add("FileFormat", typeof(string));
            dt.Columns.Add("LineNumber", typeof(string));
            dt.Columns.Add("IsSuccess", typeof(string));
            dt.Columns.Add("TransactionCode", typeof(string));
            dt.Columns.Add("CardholderNumber", typeof(string));
            dt.Columns.Add("SourceNumber", typeof(string));
            dt.Columns.Add("ExpiryDate", typeof(string));
            dt.Columns.Add("Amount", typeof(string));
            dt.Columns.Add("AmountInt", typeof(string));
            dt.Columns.Add("JulianDate", typeof(string));
            dt.Columns.Add("TransactionDate", typeof(string));
            dt.Columns.Add("TransactionTime", typeof(string));
            dt.Columns.Add("AuthCode", typeof(string));
            dt.Columns.Add("OriginatorRef", typeof(string));
            dt.Columns.Add("Length", typeof(string));
            dt.Columns.Add("UseTestCard", typeof(string));
            dt.Columns.Add("ErrorReason", typeof(string));
            dt.Columns.Add("DateProcessed", typeof(DateTime));
            return dt;
        }

        private void FillDataRow(BigQueryRow row, DataRow dataRow)
        {
            var columnsToLog = new[] { "FileName", "FileFormat", "LineNumber", "CardholderNumber", "Amount", "DateProcessed" };

            foreach (var colName in columnsToLog)
            {
                var value = row[colName];
                _logger.LogInformation("[{FileFormat}] Raw BigQuery value for '{ColName}': '{Value}' (Type: {Type})",
                    row["FileFormat"], colName, value, value?.GetType().Name ?? "null");
            }
            dataRow["FileName"] = row["FileName"] ?? DBNull.Value;
            dataRow["FileFormat"] = row["FileFormat"] ?? DBNull.Value;
            dataRow["LineNumber"] = row["LineNumber"] ?? DBNull.Value;
            dataRow["IsSuccess"] = row["IsSuccess"] ?? DBNull.Value;
            dataRow["TransactionCode"] = row["TransactionCode"] ?? DBNull.Value;
            dataRow["CardholderNumber"] = row["CardholderNumber"] ?? DBNull.Value;
            dataRow["SourceNumber"] = row["SourceNumber"] ?? DBNull.Value;
            dataRow["ExpiryDate"] = row["ExpiryDate"] ?? DBNull.Value;
            dataRow["Amount"] = row["Amount"] ?? DBNull.Value;
            dataRow["AmountInt"] = row["AmountInt"] ?? DBNull.Value;
            dataRow["JulianDate"] = row["JulianDate"] ?? DBNull.Value;
            dataRow["TransactionDate"] = row["TransactionDate"] ?? DBNull.Value;
            dataRow["TransactionTime"] = row["TransactionTime"] ?? DBNull.Value;
            dataRow["AuthCode"] = row["AuthCode"] ?? DBNull.Value;
            dataRow["OriginatorRef"] = row["OriginatorRef"] ?? DBNull.Value;
            dataRow["Length"] = row["Length"] ?? DBNull.Value;
            dataRow["UseTestCard"] = row["UseTestCard"] ?? DBNull.Value;
            dataRow["ErrorReason"] = row["ErrorReason"] ?? DBNull.Value;
            dataRow["DateProcessed"] = row["DateProcessed"] ?? DateTime.UtcNow;

            _logger.LogInformation("Filled DataRow: {dataRow}", dataRow);
        }

        private async Task<int> BulkInsert(string connectionString, DataTable dataTable, string targetTable)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();
                using (var bulk = new SqlBulkCopy(conn))
                {
                    bulk.DestinationTableName = targetTable;
                    foreach (DataColumn col in dataTable.Columns)
                        bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                    await bulk.WriteToServerAsync(dataTable);
                    return dataTable.Rows.Count;
                }
            }
        }
    }
}
