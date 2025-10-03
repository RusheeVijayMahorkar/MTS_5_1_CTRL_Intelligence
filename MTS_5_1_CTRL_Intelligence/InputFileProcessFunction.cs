using CloudNative.CloudEvents;
using Google.Cloud.BigQuery.V2;
using Google.Cloud.Functions.Framework;
using Google.Cloud.Storage.V1;
using Google.Events.Protobuf.Cloud.Storage.V1;
using Microsoft.Extensions.Logging;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace MTS_5_1_CTRL_Intelligence
{
    public class Function : ICloudEventFunction<StorageObjectData>
    {
        private readonly ILogger _logger;
        private readonly StorageClient _storageClient;
        private static readonly HttpClient _httpClient = new HttpClient();
        private HashSet<string> _seenRecords = new HashSet<string>();

        private readonly string _sendGridApiKey;
        private readonly string _emailTo;
        private readonly string _emailFrom;
        private readonly string _projectId;
        private readonly string _datasetId;
        private readonly string _emailToKf;
        private readonly string _emailToPi;

        private const string GenericTableName = "file_processed_table";

        public Function(ILogger<Function> logger)
        {
            _logger = logger;
            _storageClient = StorageClient.Create();

            _sendGridApiKey = Environment.GetEnvironmentVariable("SENDGRID_API_KEY") ?? throw new InvalidOperationException("SENDGRID_API_KEY not set.");
            _emailTo = Environment.GetEnvironmentVariable("EMAIL_TO") ?? throw new InvalidOperationException("EMAIL_TO not set.");
            _emailFrom = Environment.GetEnvironmentVariable("EMAIL_FROM") ?? throw new InvalidOperationException("EMAIL_FROM not set.");
            _projectId = Environment.GetEnvironmentVariable("GCP_PROJECT") ?? Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "";
            _datasetId = Environment.GetEnvironmentVariable("DATASET_ID") ?? throw new InvalidOperationException("DATASET_ID not set.");
            _emailToKf = Environment.GetEnvironmentVariable("EMAIL_TO_KF") ?? "";
            _emailToPi = Environment.GetEnvironmentVariable("EMAIL_TO_PI") ?? "";
        }

        public async Task HandleAsync(CloudEvent cloudEvent, StorageObjectData data, CancellationToken cancellationToken)
        {
            string bucketName = data.Bucket;
            string fileName = data.Name;

            _logger.LogInformation("Event received for {bucket}/{file}", bucketName, fileName);

            // Only trigger for .dat files (any path)
            if (string.IsNullOrEmpty(fileName) || !fileName.StartsWith("file-uploaded-from-sftp/") || !fileName.EndsWith(".dat", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("Skipping file {file}", fileName);
                return;
            }

            _logger.LogInformation("Processing file {file} from bucket {bucket}", fileName, bucketName);

            // Destination processed CSV path (single place for all results)
            string processedFileName = fileName
                                       .Replace("file-uploaded-from-sftp/", "file-processed/", StringComparison.OrdinalIgnoreCase)
                                       .Replace(".dat", "-processed.csv", StringComparison.OrdinalIgnoreCase);

            // Download file
            using var memoryStream = new MemoryStream();
            await _storageClient.DownloadObjectAsync(bucketName, fileName, memoryStream, cancellationToken: cancellationToken);
            memoryStream.Position = 0;

            var validRows = new List<string[]>();
            var failedRows = new List<FailRow>();

            // Headers for in-memory validRows (keep for internal usage)
            validRows.Add(new[] { "TransactionCode", "CardholderNumber", "SourceNumber", "ExpiryDate", "Amount", "AmountInt", "JulianDate", "TransactionDate", "TransactionTime", "AuthCode", "OriginatorRef", "Length", "UseTestCard" });

            int successCount = 0;
            int failCount = 0;

            var successSummary = new Dictionary<string, (int count, long totalAmount)>();

            using var reader = new StreamReader(memoryStream);
            string? line;
            int lineNumber = 0;
            bool isFileFormatFailed = false;

            var allLines = new List<string>();

            string fileFormat = "UNKNOWN";

            while ((line = await reader.ReadLineAsync()) != null) allLines.Add(line);

            // Validate header and footer
            if (allLines.Count == 0)
            {
                isFileFormatFailed = true;
                failedRows.Add(new FailRow { LineNumber = 0, RawLine = "", ErrorReason = "File is empty" });

                // Build single processed CSV with failed rows and upload
                await UploadProcessedCsvAndInsertBigQuery(bucketName, fileName, processedFileName, "UNKNOWN", validRows, failedRows, cancellationToken);
                return;
            }

            // Validate header
            var header = allLines.First();
            if (!header.StartsWith("VOL") || header.Length < 4)
            {
                isFileFormatFailed = true;
                failedRows.Add(new FailRow
                {
                    LineNumber = 1,
                    RawLine = header,
                    ErrorReason = "Header missing or invalid (VOLx)"
                });
            }
            else
            {
                if (header.StartsWith("VOL1")) fileFormat = "NS1";
                else if (header.StartsWith("VOL2")) fileFormat = "NS2";
                else fileFormat = "UNKNOWN";
            }

            _logger.LogInformation("Detected format {format} for file {file}", fileFormat, fileName);

            // Validate footer
            var footer = allLines.Last();
            if (!footer.StartsWith("UTL") || footer.Length < 3)
            {
                isFileFormatFailed = true;
                failedRows.Add(new FailRow
                {
                    LineNumber = allLines.Count,
                    RawLine = footer,
                    ErrorReason = "Footer missing or invalid (UTLx)"
                });
            }

            // Empty line check
            for (int i = 1; i < allLines.Count - 1; i++)
            {
                if (string.IsNullOrWhiteSpace(allLines[i]))
                {
                    isFileFormatFailed = true;
                    failedRows.Add(new FailRow { LineNumber = i + 1, RawLine = "", ErrorReason = "Empty transaction line detected" });
                }
            }

            // If header/footer/empty line checks produced fails and there are no valid transaction lines, upload processed CSV with failures and early exit if file format is invalid
            if (isFileFormatFailed)
            {
                await UploadProcessedCsvAndInsertBigQuery(bucketName, fileName, processedFileName, fileFormat, validRows, failedRows, cancellationToken);
                var summaryWithFailDetails = new ProcessingSummary
                {
                    FileName = fileName,
                    FailRows = failedRows,
                    FailCount = failedRows.Count,
                    IsFileFormatFailed = true,
                    FileFormat = fileFormat
                };

                await HandleSummaryAsync(summaryWithFailDetails, cancellationToken);
                return;
            }

            // Process transaction lines (between header and footer)
            for (int i = 1; i < allLines.Count - 1; i++)
            {
                lineNumber++;
                line = allLines[i];
                if (string.IsNullOrWhiteSpace(line)) continue;

                long amountInt;
                string sourceNumber;
                (string[] row, List<string> errors) result;

                if (fileFormat == "NS1")
                {
                    result = ProcessAndValidateLine(line, out amountInt, out sourceNumber);
                }
                else if (fileFormat == "NS2")
                {
                    // TODO: Implement NS2-specific parser. For now reuse NS1 parser as a placeholder.
                    result = ProcessAndValidateLine(line, out amountInt, out sourceNumber);
                }
                else
                {
                    failedRows.Add(new FailRow { LineNumber = lineNumber, RawLine = line, ErrorReason = "Unsupported file format" });
                    failCount++;
                    continue;
                }

                if (result.errors.Count > 0)
                {
                    // Combine all errors into one string
                    string combinedErrors = string.Join("; ", result.errors);

                    failedRows.Add(new FailRow
                    {
                        LineNumber = lineNumber,
                        RawLine = line,
                        ErrorReason = combinedErrors
                    });

                    failCount++; // increment ONCE per transaction line
                }
                else
                {
                    validRows.Add(result.row);
                    successCount++;

                    // Update success summary by Transmission Reference (SourceNumber)
                    if (!successSummary.ContainsKey(sourceNumber)) successSummary[sourceNumber] = (0, 0);
                    var current = successSummary[sourceNumber];
                    successSummary[sourceNumber] = (current.count + 1, current.totalAmount + amountInt);
                }
            }

            // Build and upload single processed CSV and insert into BigQuery
            try
            {
                await UploadProcessedCsvAndInsertBigQuery(bucketName, fileName, processedFileName, fileFormat, validRows, failedRows, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while uploading processed CSV or inserting to BigQuery: {message}", ex.Message);
            }

            // Build summary object
            var summary = new ProcessingSummary
            {
                FileName = Path.GetFileName(fileName),
                SuccessCount = successCount,
                FailCount = failCount,
                ProcessedFile = processedFileName,
                FailedFile = failedRows.Count > 0 ? processedFileName : "",
                SuccessRows = successSummary.Select(s => new SuccessRow { TransmissionRef = s.Key, TransactionCount = s.Value.count, TotalAmount = s.Value.totalAmount }).ToList(),
                FailRows = failedRows,
                IsFileFormatFailed = isFileFormatFailed,
                FileFormat = fileFormat
            };

            // Send emails based on success/fail counts (keeps existing logic)
            await HandleSummaryAsync(summary, cancellationToken);

            _logger.LogInformation("Processing complete. Success: {successCount}, Fail: {failCount}", successCount, failCount);
        }

        /// <summary>
        /// Build a single combined CSV (success + failed) and upload it to the processedFileName location,
        /// then insert the same rows into BigQuery table GenericTableName.
        /// </summary>
        private async Task UploadProcessedCsvAndInsertBigQuery(
            string bucketName,
            string originalFileName,
            string processedFileName,
            string fileFormat,
            List<string[]> successRows,
            List<FailRow> failedRows,
            CancellationToken ct)
        {
            // Build CSV rows with header matching BigQuery columns (and additional IsSuccess + ProcessedFile)
            var csvRows = new List<string[]>();

            // Header
            csvRows.Add(new[]
            {
                "FileName",
                "FileFormat",
                "ProcessedFile",
                "LineNumber",
                "IsSuccess",
                "TransactionCode",
                "CardholderNumber",
                "SourceNumber",
                "ExpiryDate",
                "Amount",
                "AmountInt",
                "JulianDate",
                "TransactionDate",
                "TransactionTime",
                "AuthCode",
                "OriginatorRef",
                "Length",
                "UseTestCard",
                "ErrorReason",
                "DateProcessed"
            });






            string dateProcessedStr = DateTime.UtcNow.ToString("o");

            // Success rows (note successRows[0] is internal header)
            for (int i = 1; i < successRows.Count; i++)
            {
                var r = successRows[i];
                csvRows.Add(new[]
                {
                    originalFileName,                   // FileName
                    fileFormat,                         // FileFormat
                    processedFileName,                  // ProcessedFile
                    "",                                 // LineNumber (not kept for valid rows)
                    "true",                             // IsSuccess
                    r.Length > 0 ? r[0] : "",           // TransactionCode
                    r.Length > 1 ? r[1] : "",           // CardholderNumber
                    r.Length > 2 ? r[2] : "",           // SourceNumber
                    r.Length > 3 ? r[3] : "",           // ExpiryDate
                    r.Length > 4 ? r[4] : "",           // Amount
                    r.Length > 5 ? r[5] : "",           // AmountInt
                    r.Length > 6 ? r[6] : "",           // JulianDate
                    r.Length > 7 ? r[7] : "",           // TransactionDate
                    r.Length > 8 ? r[8] : "",           // TransactionTime
                    r.Length > 9 ? r[9] : "",           // AuthCode
                    r.Length > 10 ? r[10] : "",         // OriginatorRef
                    r.Length > 11 ? r[11] : "",         // Length
                    r.Length > 12 ? r[12] : "",         // UseTestCard
                    "",                                 // ErrorReason
                    dateProcessedStr                    // DateProcessed
                });
            }

            // Failed rows
            foreach (var f in failedRows)
            {
                csvRows.Add(new[]
                {
                    originalFileName,          // FileName
                    fileFormat,                // FileFormat
                    processedFileName,         // ProcessedFile
                    f.LineNumber.ToString(),   // LineNumber
                    "false",                   // IsSuccess
                    "",                        // TransactionCode
                    "",                        // CardholderNumber
                    "",                        // SourceNumber
                    "",                        // ExpiryDate
                    "",                        // Amount
                    "",                        // AmountInt
                    "",                        // JulianDate
                    "",                        // TransactionDate
                    "",                        // TransactionTime
                    "",                        // AuthCode
                    "",                        // OriginatorRef
                    "",                        // Length
                    "",                        // UseTestCard
                    f.ErrorReason ?? "",       // ErrorReason
                    dateProcessedStr           // DateProcessed
                });
            }

            // Upload combined CSV to bucket (single processed file)
            await UploadCsv(csvRows, bucketName, processedFileName, ct);
            _logger.LogInformation("Uploaded combined processed CSV to {file}", processedFileName);

            // Also insert rows into BigQuery
            await InsertDailyRecordsAsync(originalFileName, fileFormat, successRows, failedRows, ct);
        }

        private async Task InsertDailyRecordsAsync(string fileName, string fileFormat, List<string[]> successRows, List<FailRow> failedRows, CancellationToken ct)
        {
            var bigQueryClient = BigQueryClient.Create(_projectId);
            var tableRef = bigQueryClient.GetTable(_datasetId, GenericTableName);
            var rows = new List<BigQueryInsertRow>();

            // Success rows
            for (int i = 1; i < successRows.Count; i++)
            {
                var row = successRows[i];
                rows.Add(new BigQueryInsertRow
                {
                    {"FileName", fileName},
                    {"FileFormat", fileFormat},
                    {"LineNumber", null},
                    {"IsSuccess", "true"},
                    {"TransactionCode", row.Length > 0 ? row[0] : null},
                    {"CardholderNumber", row.Length > 1 ? row[1] : null},
                    {"SourceNumber", row.Length > 2 ? row[2] : null},
                    {"ExpiryDate", row.Length > 3 ? row[3] : null},
                    {"Amount", row.Length > 4 ? row[4] : null},
                    {"AmountInt", row.Length > 5 && long.TryParse(row[5], out var amt) ? (object)amt : null},
                    {"JulianDate", row.Length > 6 ? row[6] : null},
                    {"TransactionDate", row.Length > 7 ? row[7] : null},
                    {"TransactionTime", row.Length > 8 ? row[8] : null},
                    {"AuthCode", row.Length > 9 ? row[9] : null},
                    {"OriginatorRef", row.Length > 10 ? row[10] : null},
                    {"Length", row.Length > 11 && long.TryParse(row[11], out var len) ? (object)len : null},
                    {"UseTestCard", row.Length > 12 ? row[12] : null},
                    {"ErrorReason", null},
                    {"DateProcessed", DateTime.UtcNow}
                });
            }

            // Failed rows
            foreach (var f in failedRows)
            {
                rows.Add(new BigQueryInsertRow
                {
                    {"FileName", fileName},
                    {"FileFormat", fileFormat},
                    {"LineNumber", f.LineNumber},
                    {"IsSuccess", "false"},
                    {"TransactionCode", null},
                    {"CardholderNumber", null},
                    {"SourceNumber", null},
                    {"ExpiryDate", null},
                    {"Amount", null},
                    {"AmountInt", null},
                    {"JulianDate", null},
                    {"TransactionDate", null},
                    {"TransactionTime", null},
                    {"AuthCode", null},
                    {"OriginatorRef", null},
                    {"Length", null},
                    {"UseTestCard", null},
                    {"ErrorReason", f.ErrorReason},
                    {"DateProcessed", DateTime.UtcNow}
                });
            }

            if (rows.Count > 0)
                await tableRef.InsertRowsAsync(rows, cancellationToken: ct);

            _logger.LogInformation("Inserted {count} rows into BigQuery table {table}", rows.Count, GenericTableName);
        }

        private async Task UploadCsv(List<string[]> rows, string bucketName, string targetFile, CancellationToken cancellationToken)
        {
            var sb = new StringBuilder();
            foreach (var row in rows)
            {
                // handle nulls defensively
                var cols = row.Select(c => c ?? "").ToArray();
                for (int i = 0; i < cols.Length; i++)
                {
                    if (cols[i].Contains(",") || cols[i].Contains("\""))
                        cols[i] = $"\"{cols[i].Replace("\"", "\"\"")}\"";
                }
                sb.AppendLine(string.Join(",", cols));
            }

            using var outStream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
            await _storageClient.UploadObjectAsync(bucketName, targetFile, "text/csv", outStream, cancellationToken: cancellationToken);
            _logger.LogInformation("Uploaded CSV: {file}", targetFile);
        }

        private (string[] row, List<string> errors) ProcessAndValidateLine(string line, out long amountInt, out string sourceNumber)
        {
            var errors = new List<string>();
            amountInt = 0;
            sourceNumber = "";

            // --- Extract fields as per new spec ---
            string cardholderNumber = SafeSubstring(line, 0, 19);       // n19
            string transactionCode = SafeSubstring(line, 19, 2);       // a2
            sourceNumber = SafeSubstring(line, 21, 11);      // n11
            string expiryDate = SafeSubstring(line, 32, 4);       // n4 (MMYY)
            string amount = SafeSubstring(line, 36, 11);      // n11
            string julianDate = SafeSubstring(line, 47, 6);       // a6 (YYDDD)
            string transactionTime = SafeSubstring(line, 53, 6);       // n6 (HHMMSS)
            string authCode = SafeSubstring(line, 61, 6);       // a6
            string originatorRef = SafeSubstring(line, 67, 12);

            // --- Validations ---
            if (cardholderNumber.Length != 19 || !long.TryParse(cardholderNumber, out _))
                errors.Add($"CardholderNumber '{cardholderNumber}' invalid. Expected 19 digits numeric.");

            if (transactionCode != "E1" && transactionCode != "E2")
                errors.Add($"TransactionCode '{transactionCode}' invalid. Expected E1 or E2.");

            if (sourceNumber.Length != 11 || !long.TryParse(sourceNumber, out _))
                errors.Add($"SourceNumber '{sourceNumber}' invalid. Expected 11 digits numeric.");

            if (expiryDate.Length != 4 || !int.TryParse(expiryDate, out _))
            {
                errors.Add($"Card ExpiryDate '{expiryDate}' invalid. Expected MMYY numeric.");
            }
            else
            {
                string mm = expiryDate.Substring(0, 2);
                if (!int.TryParse(mm, out int month) || month < 1 || month > 12)
                    errors.Add($"Card ExpiryDate '{expiryDate}' invalid. Month must be between 01 and 12.");
            }

            // Amount: numeric and >0 (in pence)
            if (!long.TryParse(amount, out amountInt) || amountInt <= 0)
                errors.Add($"Amount '{amount}' invalid. Expected positive integer.");

            // Julian date: accept trimmed YYDDD (ignore leading space if present)
            string julian = (julianDate ?? "").Trim();
            string transactionDateConverted = ""; // ddMMyy if conversion succeeds
            if (!Regex.IsMatch(julian, @"^\d{5}$"))
            {
                errors.Add($"TransactionDate '{julianDate}' invalid. Expected YYDDD (Julian day).");
            }
            else
            {
                // validate day-of-year
                string yyPart = julian.Substring(0, 2);
                string dddPart = julian.Substring(2, 3);
                if (!int.TryParse(yyPart, out int yy) || !int.TryParse(dddPart, out int ddd))
                {
                    errors.Add($"Julian TransactionDate '{julianDate}' invalid. YY or DDD not numeric.");
                }
                else if (ddd < 1 || ddd > 366)
                {
                    errors.Add($"Julian TransactionDate '{julianDate}' invalid. Julian day {ddd} out of range (1-366).");
                }
                else
                {
                    // convert to ddMMyy; use existing converter which expects YYDDD or similar
                    transactionDateConverted = ConvertJulianTransactionDate(julian);
                    if (string.IsNullOrEmpty(transactionDateConverted))
                        errors.Add($"TransactionDate '{julianDate}' could not be converted to calendar date.");
                }
            }

            if (transactionTime.Length != 6 || !int.TryParse(transactionTime, out _))
            {
                errors.Add($"TransactionTime '{transactionTime}' invalid. Expected HHMMSS numeric.");
            }
            else
            {
                int hh = int.Parse(transactionTime.Substring(0, 2));
                int mm = int.Parse(transactionTime.Substring(2, 2));
                int ss = int.Parse(transactionTime.Substring(4, 2));
                if (hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59)
                    errors.Add($"TransactionTime '{transactionTime}' invalid. Must be valid 24h HHMMSS.");
            }

            // AuthCode: allow alphanumeric or spaces only (no length enforcement)
            if (!Regex.IsMatch(authCode, @"^[A-Za-z0-9 ]*$"))
                errors.Add($"AuthCode '{authCode}' invalid. Expected only alphanumeric characters or spaces.");

            int length = line.Length;
            string useTestCard = "Y";

            // --- Duplicate check: only if record passed all above validations ---
            if (errors.Count == 0)
            {
                string recordKey = $"{cardholderNumber}|{sourceNumber}|{transactionDateConverted}|{transactionTime}|{amountInt}";
                if (_seenRecords.Contains(recordKey))
                {
                    errors.Add($"Duplicate record detected for key {recordKey}.");
                }
                else
                {
                    _seenRecords.Add(recordKey);
                }
            }

            // --- Build row for valid records ---
            var row = new[]
            {
                transactionCode,
                cardholderNumber,
                sourceNumber,
                expiryDate,
                amount,
                amountInt.ToString(),
                julian,                      // store the raw/trimmed julian value
                transactionDateConverted,    // converted ddMMyy (may be empty if conversion failed)
                transactionTime,
                authCode,
                originatorRef,
                length.ToString(),
                useTestCard
            };

            return (row, errors);
        }

        private async Task HandleSummaryAsync(ProcessingSummary summary, CancellationToken ct)
        {
            string emailRecipient;

            if (string.IsNullOrEmpty(summary.FileFormat))
            {
                // file structure failed, no format → fallback
                emailRecipient = _emailTo;
            }
            else if (summary.FileFormat == "NS1")
            {
                emailRecipient = string.IsNullOrEmpty(_emailToKf) ? _emailTo : _emailToKf;
            }
            else if (summary.FileFormat == "NS2")
            {
                emailRecipient = string.IsNullOrEmpty(_emailToPi) ? _emailTo : _emailToPi;
            }
            else
            {
                // unsupported format → fallback
                emailRecipient = _emailTo;
            }

            string fileName = summary.FileName;
            string dateStr = DateTime.UtcNow.ToString("yyyy-MM-dd");
            string timeStr = DateTime.UtcNow.ToString("HH:mm:ss") + " UTC";

            // CASE 1: File format failure
            if (summary.IsFileFormatFailed)
            {
                string reasons = string.Join("; ", summary.FailRows.Select(r => r.ErrorReason));
                string subject = $"[ALERT] File {fileName} could not be processed (File Format Error)";
                string bodyHtml = $@"
            <p>Hello,</p>
            <p>The file <strong>{EscapeHtml(fileName)}</strong> was received on {dateStr} at {timeStr}.</p>
            <p>The file could not be processed due to the following reason(s): <strong>{EscapeHtml(reasons)}</strong></p>
            <p>Thanks,<br/>Worldline Support team</p>";

                await SendEmailViaSendGrid(subject, bodyHtml, ct, emailRecipient);
                return;
            }

            _logger.LogInformation("File {file} processed with {success} successes and {fail} failures.", fileName, summary.SuccessCount, summary.FailCount);
            // CASE 2: Failed transactions (row-level failures)
            if (summary.FailCount > 0)
            {
                // Build summary table (success + failed grouped by Transmission Reference)
                var summaryTable = new StringBuilder();
                summaryTable.Append("<table border='1' cellpadding='5' cellspacing='0'>");
                summaryTable.Append("<tr><th>Source Reference</th><th>Total Transactions</th><th>Total Amount</th><th>Passed Count</th><th>Passed Amount</th><th>Failed Count</th><th>Failed Amount</th></tr>");

                // Group by TransmissionRef
                var grouped = summary.SuccessRows
                    .GroupBy(r => r.TransmissionRef)
                    .ToDictionary(g => g.Key, g => g.ToList());

                long grandTotalAmount = 0, grandPassAmount = 0, grandFailAmount = 0;
                int grandTotalCount = 0, grandPassCount = 0, grandFailCount = 0;

                foreach (var key in grouped.Keys)
                {
                    var successRows = grouped[key];
                    int passCount = successRows.Sum(r => r.TransactionCount);
                    long passAmount = successRows.Sum(r => r.TotalAmount);

                    // Failures for this source
                    var failRows = summary.FailRows.Where(f => f.RawLine.Contains(key)).ToList();
                    int failCount = failRows.Count;
                    long failAmount = 0;

                    int totalCount = passCount + failCount;
                    long totalAmount = passAmount + failAmount;

                    summaryTable.Append($"<tr><td>{EscapeHtml(key)}</td><td>{totalCount}</td><td>{totalAmount}</td><td>{passCount}</td><td>{passAmount}</td><td>{failCount + 1}</td><td>{failAmount}</td></tr>");

                    grandTotalCount += totalCount;
                    grandTotalAmount += totalAmount;
                    grandPassCount += passCount;
                    grandPassAmount += passAmount;
                    grandFailCount += failCount;
                    grandFailAmount += failAmount;
                }

                // Totals row
                summaryTable.Append($"<tr><th>Total</th><th>{grandTotalCount}</th><th>{grandTotalAmount}</th><th>{grandPassCount}</th><th>{grandPassAmount}</th><th>{grandFailCount + 1}</th><th>{grandFailAmount}</th></tr>");
                summaryTable.Append("</table>");

                // Failed transactions detail table
                var failTable = new StringBuilder();
                failTable.Append("<table border='1' cellpadding='5' cellspacing='0'>");
                failTable.Append("<tr><th>Line Number</th><th>Raw Line</th><th>Error Reason</th></tr>");
                foreach (var row in summary.FailRows)
                {
                    failTable.Append($"<tr><td>{row.LineNumber}</td><td>{EscapeHtml(row.RawLine)}</td><td>{EscapeHtml(row.ErrorReason)}</td></tr>");
                }
                failTable.Append("</table>");

                string subject = $"[ALERT] File {fileName} processed with failures ({summary.FailCount})";
                string bodyHtml = $@"
                                    <p>Hello,</p>
                                    <p>The file <strong>{EscapeHtml(fileName)}</strong> was received on {dateStr} at {timeStr}.</p>
                                    <p>There were <strong>{summary.FailCount}</strong> failed transactions in the file.</p>
                                    <p>Please see the summary of the transactions below:</p>
                                    {summaryTable}
                                    <p>Details of Failed transactions are listed below:</p>
                                    {failTable}
                                    <p>Thanks,<br/>Worldline Support team</p>";

                await SendEmailViaSendGrid(subject, bodyHtml, ct, emailRecipient);
                return;
            }

            _logger.LogInformation("File {file} processed successfully with no failures. Success count: {successCount}", fileName, summary.SuccessCount);
            // CASE 3: All passed
            if (summary.SuccessCount > 0 && summary.FailCount == 0)
            {
                var successTable = new StringBuilder();
                successTable.Append("<table border='1' cellpadding='5' cellspacing='0'>");
                successTable.Append("<tr><th>Transmission Reference</th><th>No. of Transactions</th><th>Total Amount</th></tr>");
                foreach (var row in summary.SuccessRows)
                {
                    successTable.Append($"<tr><td>{EscapeHtml(row.TransmissionRef)}</td><td>{row.TransactionCount}</td><td>{row.TotalAmount}</td></tr>");
                }
                successTable.Append("</table>");

                string subject = $"[OK] File {fileName} processed successfully ({summary.SuccessCount})";
                string bodyHtml = $@"
            <p>Hello,</p>
            <p>The file <strong>{EscapeHtml(fileName)}</strong> received on {dateStr} at {timeStr} is processed successfully.</p>
            <p>Please see the summary of the transactions below:</p>
            {successTable}
            <p>Thanks,<br/>Worldline Support team</p>";

                await SendEmailViaSendGrid(subject, bodyHtml, ct, emailRecipient);
            }
        }

        private async Task SendEmailViaSendGrid(string subject, string htmlBody, CancellationToken ct, string emailRecipient)
        {
            var sendGridPayload = new
            {
                personalizations = new[] { new { to = new[] { new { email = emailRecipient } } } },
                from = new { email = _emailFrom },
                subject = subject,
                content = new[] { new { type = "text/html", value = htmlBody } }
            };

            var request = new HttpRequestMessage(HttpMethod.Post, "https://api.sendgrid.com/v3/mail/send")
            {
                Content = new StringContent(JsonSerializer.Serialize(sendGridPayload), Encoding.UTF8, "application/json")
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _sendGridApiKey);

            var response = await _httpClient.SendAsync(request, ct);
            var respBody = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError("SendGrid API call failed (Status: {status}). Body: {body}", response.StatusCode, respBody);
                throw new InvalidOperationException($"SendGrid API returned {response.StatusCode}");
            }

            _logger.LogInformation("Email sent successfully to {to}. Subject: {subject}", _emailTo, subject);
        }

        private static string EscapeHtml(string? input) => string.IsNullOrEmpty(input) ? "" : System.Net.WebUtility.HtmlEncode(input);

        private static string SafeSubstring(string input, int start, int length)
        {
            if (string.IsNullOrEmpty(input) || start >= input.Length) return "";
            if (start + length > input.Length) length = input.Length - start;
            return input.Substring(start, length).Trim();
        }

        private string ConvertJulianTransactionDate(string julian)
        {
            if (string.IsNullOrWhiteSpace(julian) || julian.Length < 5)
                return "";

            try
            {
                int year = int.Parse(julian.Substring(0, 2));
                int dayOfYear = int.Parse(julian.Substring(julian.Length - 3));
                int fullYear = 2000 + year;
                DateTime date = new DateTime(fullYear, 1, 1).AddDays(dayOfYear - 1);
                return date.ToString("ddMMyy");
            }
            catch
            {
                return "";
            }
        }

        private class ProcessingSummary
        {
            public string FileName { get; set; } = "";
            public int SuccessCount { get; set; }
            public int FailCount { get; set; }
            public string ProcessedFile { get; set; } = "";
            public string FailedFile { get; set; } = "";
            public List<SuccessRow> SuccessRows { get; set; } = new();
            public List<FailRow> FailRows { get; set; } = new();
            public bool IsFileFormatFailed { get; set; } = false;
            public string FileFormat { get; set; } = "";
        }

        private class SuccessRow
        {
            public string TransmissionRef { get; set; } = "";
            public int TransactionCount { get; set; }
            public long TotalAmount { get; set; }
        }

        private class FailRow
        {
            public int LineNumber { get; set; }
            public string RawLine { get; set; } = "";
            public string ErrorReason { get; set; } = "";
        }
    }
}
