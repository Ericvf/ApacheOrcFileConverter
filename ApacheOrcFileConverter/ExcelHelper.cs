using DocumentFormat.OpenXml.Packaging;
using System.Data;

namespace ApacheOrcFileConverter
{
    public static class ExcelHelper
    {
        public static void OrcFileReaderToExcel(OrcFileReader orcReader, string destination)
        {
            using (var workbook = SpreadsheetDocument.Create(destination, DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook))
            {
                var workbookPart = workbook.AddWorkbookPart();
                workbook.WorkbookPart.Workbook = new DocumentFormat.OpenXml.Spreadsheet.Workbook();
                workbook.WorkbookPart.Workbook.Sheets = new DocumentFormat.OpenXml.Spreadsheet.Sheets();

                uint sheetId = 1;

                var sheetPart = workbook.WorkbookPart.AddNewPart<WorksheetPart>();
                var sheetData = new DocumentFormat.OpenXml.Spreadsheet.SheetData();
                sheetPart.Worksheet = new DocumentFormat.OpenXml.Spreadsheet.Worksheet(sheetData);

                DocumentFormat.OpenXml.Spreadsheet.Sheets sheets = workbook.WorkbookPart.Workbook.GetFirstChild<DocumentFormat.OpenXml.Spreadsheet.Sheets>();
                string relationshipId = workbook.WorkbookPart.GetIdOfPart(sheetPart);

                if (sheets.Elements<DocumentFormat.OpenXml.Spreadsheet.Sheet>().Count() > 0)
                {
                    sheetId = sheets.Elements<DocumentFormat.OpenXml.Spreadsheet.Sheet>().Select(s => s.SheetId.Value).Max() + 1;
                }

                DocumentFormat.OpenXml.Spreadsheet.Sheet sheet = new DocumentFormat.OpenXml.Spreadsheet.Sheet()
                {
                    Id = relationshipId,
                    SheetId = sheetId,
                    Name = "Sheet1",
                };

                sheets.Append(sheet);

                DocumentFormat.OpenXml.Spreadsheet.Row headerRow = new DocumentFormat.OpenXml.Spreadsheet.Row();

                List<string> columns = new List<string>();
                foreach (var header in orcReader.Headers)
                {
                    columns.Add(header);

                    DocumentFormat.OpenXml.Spreadsheet.Cell cell = new DocumentFormat.OpenXml.Spreadsheet.Cell();
                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.String;
                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(header);
                    headerRow.AppendChild(cell);
                }

                sheetData.AppendChild(headerRow);

                while (orcReader.Read())
                {
                    DocumentFormat.OpenXml.Spreadsheet.Row newRow = new DocumentFormat.OpenXml.Spreadsheet.Row();

                    for (int i = 0; i < orcReader.FieldCount; i++)
                    {
                        DocumentFormat.OpenXml.Spreadsheet.Cell cell = new DocumentFormat.OpenXml.Spreadsheet.Cell();

                        object value = orcReader.GetValue(i);
                        if (value == null)
                        {
                            cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue();
                        }
                        else
                        {

                            switch (orcReader.Types[i])
                            {
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Boolean:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Boolean;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToBoolean(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Byte:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToByte(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Short:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToInt16(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Int:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToInt32(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Long:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToInt32(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Float:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToDouble(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Double:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToDouble(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.String:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.String;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToString(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Binary:
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Timestamp:
                                    if (value is string sTring)
                                    {
                                        cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.String;
                                        cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToString(sTring));
                                    }
                                    
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.List:
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Map:
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Struct:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToInt32(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Union:
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Decimal:
                                    if (value is DateTime dateTime)
                                    {
                                        cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Date;
                                        cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToDateTime(value));
                                    }
                                    else
                                    {
                                        cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                                        cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToDecimal(value));
                                    }
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Date:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.Date;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToDateTime(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Varchar:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.String;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToString(value));
                                    break;
                                case ApacheOrcDotNet.Protocol.ColumnTypeKind.Char:
                                    cell.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.String;
                                    cell.CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(Convert.ToChar(value));
                                    break;
                                default:
                                    break;
                            }
                        }

                        newRow.AppendChild(cell);
                    }

                    sheetData.AppendChild(newRow);
                }
            }
        }
    }
}
