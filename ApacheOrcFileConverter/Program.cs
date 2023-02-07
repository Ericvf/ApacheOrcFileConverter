using ApacheOrcFileConverter;
using System.Diagnostics;

var filePath = args[0];

if (!File.Exists(filePath))
{
    MessageBox.Show($"Unable to locate `{filePath}`");
}

using var fileStream = File.OpenRead(filePath);

var orcReader = new OrcFileReader(fileStream, CancellationToken.None);

var newFileName = Path.ChangeExtension(filePath, ".orc.xlsx");

ExcelHelper.OrcFileReaderToExcel(orcReader, newFileName);

OpenWithDefaultProgram(newFileName);

static void OpenWithDefaultProgram(string path)
{
    using Process fileopener = new Process();

    fileopener.StartInfo.FileName = "explorer";
    fileopener.StartInfo.Arguments = "\"" + path + "\"";
    fileopener.Start();
}