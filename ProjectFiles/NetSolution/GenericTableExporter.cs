#region Using directives
using System;
using System.IO;
using System.Text;
using FTOptix.CoreBase;
using FTOptix.HMIProject;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.NetLogic;
using FTOptix.EventLogger;
using FTOptix.OPCUAServer;
using FTOptix.UI;
using FTOptix.Store;
using FTOptix.SQLiteStore;
using FTOptix.Core;
using FTOptix.ODBCStore;
using FTOptix.MicroController;
using FTOptix.CommunicationDriver;
using FTOptix.AuditSigning;
using FTOptix.Alarm;
using FTOptix.RAEtherNetIP;
#endregion

public class GenericTableExporter : BaseNetLogic
{
    [ExportMethod]
    public void Export()
    {
        try
        {
            var csvPath = GetCSVFilePath();
            if (string.IsNullOrEmpty(csvPath))
                throw new Exception("No CSV file chosen, please fill the CSVPath variable");

            char? fieldDelimiter = GetFieldDelimiter();
            bool wrapFields = GetWrapFields();
            var tableObject = GetTable();
            var storeObject = GetStoreObject(tableObject);
            var selectQuery = GetQuery();

            storeObject.Query(selectQuery, out string[] header, out object[,] resultSet);

            if (header == null || resultSet == null)
                throw new Exception("Unable to execute SQL query, malformed result");

            var rowCount = resultSet.GetLength(0);
            var columnCount = resultSet.GetLength(1);

            using (var csvWriter = new CSVFileWriter(csvPath) { FieldDelimiter = fieldDelimiter.Value, WrapFields = wrapFields })
            {
                var Jace = Project.Current.GetVariable("Model/Reports/JaceNameFirstReport").Value;
                var Meter = Project.Current.GetVariable("Model/Reports/MeterNameFirstReport1").Value;
                var Datefrom = Project.Current.GetVariable("Model/Reports/DailyDateFrom").Value;
                var Dateto = Project.Current.GetVariable("Model/Reports/Daily Date To").Value;
                string jace = Jace;
                string meter = Meter;
                string datefrom = Datefrom;
                string dateto = Dateto;
                // Write custom header message
                csvWriter.WriteLine(new string[] { "Daily Consumption Report" });
                csvWriter.WriteLine(new string[] { "" }); // Empty line for spacing

                // Write dynamic details (can be variables, example given here) 
                csvWriter.WriteLine(new string[] { $"Jace: {jace}" });
                csvWriter.WriteLine(new string[] { $"Meter: {meter}" });
                csvWriter.WriteLine(new string[] { $"From: {datefrom}, To: {dateto}" });
                csvWriter.WriteLine(new string[] { "" }); // Empty line for spacing
                csvWriter.WriteLine(header);
                WriteTableContent(resultSet, rowCount, columnCount, csvWriter);
            }

            Log.Info("GenericTableExporter", "The table " + tableObject.BrowseName + " has been successfully exported to " + csvPath);
        }
        catch (Exception ex)
        {
            Log.Error("GenericTableExporter", "Unable to export table: " + ex.Message);
        }
    }

    private void WriteTableContent(object[,] resultSet, int rowCount, int columnCount, CSVFileWriter csvWriter)
    {
        for (var r = 0; r < rowCount; ++r)
        {
            var currentRow = new string[columnCount];

            for (var c = 0; c < columnCount; ++c)
                currentRow[c] = resultSet[r, c]?.ToString() ?? "NULL";

            csvWriter.WriteLine(currentRow);
        }
    }

    private Table GetTable()
    {
        var alarmEventLoggerVariable = LogicObject.GetVariable("Table");

        if (alarmEventLoggerVariable == null)
            throw new Exception("Table variable not found");

        NodeId tableNodeId = alarmEventLoggerVariable.Value;
        if (tableNodeId == null || tableNodeId == NodeId.Empty)
            throw new Exception("Table variable is empty");

        var tableNode = InformationModel.Get(tableNodeId) as Table;

        if (tableNode == null)
            throw new Exception("The specified table node is not an instance of Store Table type");

        return tableNode;
    }

    private Store GetStoreObject(Table tableNode)
    {
        return tableNode.Owner.Owner as Store;
    }

    private string GetCSVFilePath()
    {
        var csvPathVariable = LogicObject.GetVariable("CSVPath");
        if (csvPathVariable == null)
            throw new Exception("CSVPath variable not found");

        return new ResourceUri(csvPathVariable.Value).Uri;
    }

    private char GetFieldDelimiter()
    {
        var separatorVariable = LogicObject.GetVariable("FieldDelimiter");
        if (separatorVariable == null)
            throw new Exception("FieldDelimiter variable not found");

        string separator = separatorVariable.Value;

        if (separator == String.Empty)
            throw new Exception("FieldDelimiter variable is empty");

        if (separator.Length != 1)
            throw new Exception("Wrong FieldDelimiter configuration. Please insert a single character");

        if (!char.TryParse(separator, out char result))
            throw new Exception("Wrong FieldDelimiter configuration. Please insert a char");

        return result;
    }

    private bool GetWrapFields()
    {
        var wrapFieldsVariable = LogicObject.GetVariable("WrapFields");
        if (wrapFieldsVariable == null)
            throw new Exception("WrapFields variable not found");

        return wrapFieldsVariable.Value;
    }

    private string GetQuery()
    {
        var queryVariable = LogicObject.GetVariable("Query");
        if (queryVariable == null)
            throw new Exception("Query variable not found");

        string query = queryVariable.Value;

        if (String.IsNullOrEmpty(query))
            throw new Exception("Query variable is empty or not valid");

        return query;
    }

    #region CSVFileWriter
    private class CSVFileWriter : IDisposable
    {
        public char FieldDelimiter { get; set; } = ',';

        public char QuoteChar { get; set; } = '"';

        public bool WrapFields { get; set; } = false;

        public CSVFileWriter(string filePath)
        {
            streamWriter = new StreamWriter(filePath, false, System.Text.Encoding.UTF8);
        }

        public CSVFileWriter(string filePath, System.Text.Encoding encoding)
        {
            streamWriter = new StreamWriter(filePath, false, encoding);
        }

        public CSVFileWriter(StreamWriter streamWriter)
        {
            this.streamWriter = streamWriter;
        }

        public void WriteLine(string[] fields)
        {
            var stringBuilder = new StringBuilder();

            for (var i = 0; i < fields.Length; ++i)
            {
                if (WrapFields)
                    stringBuilder.AppendFormat("{0}{1}{0}", QuoteChar, EscapeField(fields[i]));
                else
                    stringBuilder.AppendFormat("{0}", fields[i]);

                if (i != fields.Length - 1)
                    stringBuilder.Append(FieldDelimiter);
            }

            streamWriter.WriteLine(stringBuilder.ToString());
            streamWriter.Flush();
        }

        private string EscapeField(string field)
        {
            var quoteCharString = QuoteChar.ToString();
            return field.Replace(quoteCharString, quoteCharString + quoteCharString);
        }

        private StreamWriter streamWriter;

        #region IDisposable Support
        private bool disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                streamWriter.Dispose();

            disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
    #endregion
}
