using Accessibility;
using ApacheOrcDotNet;
using ApacheOrcDotNet.ColumnTypes;
using System.Data;
using BinaryReader = ApacheOrcDotNet.ColumnTypes.BinaryReader;
using ColumnTypeKind = ApacheOrcDotNet.Protocol.ColumnTypeKind;
using IEnumerator = System.Collections.IEnumerator;
using StringReader = ApacheOrcDotNet.ColumnTypes.StringReader;

public class OrcFileReader : IDataReader
{
    private readonly CancellationToken _cancellationToken;
    private readonly FileTail _fileTail;
    private readonly string[] _headers;
    private readonly ColumnTypeKind[] _types;
    private IEnumerator[] _columnReaders;
    private object[] _rowBuffer;
    private ulong rowIndex = 0;
    private int stripeIndex = 0;
    private ulong stripeRowIndex = 0;
    private readonly ulong totalAmountOfRecords = 0;

    public OrcFileReader(Stream stream, CancellationToken cancellationToken, long skipLineAfterHeaders = 0)
    {
        _cancellationToken = cancellationToken;
        _fileTail = new FileTail(stream);

        if (_fileTail.Stripes.Count == 0)
        {
            // No stripes == no rows in the file
            return;
        }

        totalAmountOfRecords = _fileTail.Footer.NumberOfRows;
        FieldCount = _fileTail.Footer.Types[0].SubTypes.Count();
        _headers = _fileTail.Footer.Types[0].FieldNames.ToArray();

        _types = _fileTail.Footer.Types.Select(t => t.Kind).ToArray();  

        // Arbitrary part support
        if (skipLineAfterHeaders > 0)
        {
            totalAmountOfRecords -= (ulong)skipLineAfterHeaders;
            while (skipLineAfterHeaders > 0)
            {
                var stripe = _fileTail.Stripes[stripeIndex];
                if ((ulong)skipLineAfterHeaders >= stripe.NumRows)
                {
                    skipLineAfterHeaders = (long)((ulong)skipLineAfterHeaders - stripe.NumRows);

                    stripeRowIndex = 0;
                    stripeIndex++;
                }
                else
                {
                    stripeRowIndex = (ulong)skipLineAfterHeaders - 1;
                    skipLineAfterHeaders = 0;
                }
            }
        }

        InitializeColumnReaders();
    }

#pragma warning disable CA1065 // Do not raise exceptions in unexpected locations
    public int Depth => throw new NotImplementedException();
#pragma warning restore CA1065 // Do not raise exceptions in unexpected locations

    public int FieldCount { get; }

#pragma warning disable CA1065 // Do not raise exceptions in unexpected locations
    public bool IsClosed => throw new NotImplementedException();
#pragma warning restore CA1065 // Do not raise exceptions in unexpected locations

    public int RecordsAffected => 0;

    public string[] Headers => _headers;

    public ColumnTypeKind[] Types => _types;

    public object this[int i] => GetValue(i);

#pragma warning disable CA1065 // Do not raise exceptions in unexpected locations
    public object this[string name] => throw new NotImplementedException();
#pragma warning restore CA1065 // Do not raise exceptions in unexpected locations

    public void Close()
    {
    }

    public void Dispose()
    {
        Dispose(true);
    }

    public bool GetBoolean(int i) => (bool)_rowBuffer[i];

    public byte GetByte(int i) => (byte)_rowBuffer[i];

    public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();

    public char GetChar(int i) => (char)_rowBuffer[i];

    public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();

    public IDataReader GetData(int i) => throw new NotImplementedException();

    public string GetDataTypeName(int i) => throw new NotImplementedException();

    public DateTime GetDateTime(int i) => (DateTime)_rowBuffer[i];

    public decimal GetDecimal(int i) => (decimal)_rowBuffer[i];

    public double GetDouble(int i) => (double)_rowBuffer[i];

    public Type GetFieldType(int i)
    {
        var columnType = _fileTail.Footer.Types[i].Kind;
        switch (columnType)
        {
            case ColumnTypeKind.Boolean:
                return typeof(bool);

            case ColumnTypeKind.Byte:
                return typeof(byte);

            case ColumnTypeKind.Short:
                return typeof(short);

            case ColumnTypeKind.Int:
                return typeof(int);

            case ColumnTypeKind.Long:
                return typeof(long);

            case ColumnTypeKind.Float:
                return typeof(float);

            case ColumnTypeKind.Double:
                return typeof(double);

            case ColumnTypeKind.String:
                return typeof(string);

            case ColumnTypeKind.Timestamp:
                return typeof(TimeSpan);

            case ColumnTypeKind.Decimal:
                return typeof(decimal);

            case ColumnTypeKind.Date:
                return typeof(DateTime);

            case ColumnTypeKind.Varchar:
                return typeof(string);

            case ColumnTypeKind.Char:
                return typeof(char);

            case ColumnTypeKind.Binary:
            case ColumnTypeKind.List:
            case ColumnTypeKind.Map:
            case ColumnTypeKind.Struct:
            case ColumnTypeKind.Union:
            default:
                throw new NotImplementedException();
        }
    }

    public float GetFloat(int i) => (float)_rowBuffer[i];

    public Guid GetGuid(int i) => (Guid)_rowBuffer[i];

    public short GetInt16(int i) => (short)_rowBuffer[i];

    public int GetInt32(int i) => (int)_rowBuffer[i];

    public long GetInt64(int i) => (long)_rowBuffer[i];

    public string GetName(int i)
    {
        return _headers[i];
    }

    public int GetOrdinal(string name)
    {
        var ordinal = 0;
        foreach (var column in _headers)
        {
            if (string.Equals(column, name, StringComparison.InvariantCultureIgnoreCase))
            {
                return ordinal;
            }

            ordinal++;
        }

        return -1;
    }

    public DataTable GetSchemaTable() => throw new NotImplementedException();

    public string GetString(int i) => throw new NotImplementedException();

    public object GetValue(int i)
    {
        return _rowBuffer[i];
    }

    public int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    public bool IsDBNull(int i) => throw new NotImplementedException();

    public bool NextResult()
    {
        throw new NotImplementedException();
    }

    public bool Read()
    {
        if (_fileTail.Stripes.Count == 0)
        {
            // No stripes == no rows in the file
            return false;
        }

        var stripe = _fileTail.Stripes[stripeIndex];
        if (stripeRowIndex == stripe.NumRows)
        {
            stripeRowIndex = 0;
            stripeIndex++;
            if (stripeIndex == _fileTail.Stripes.Count)
            {
                return false;
            }
            InitializeColumnReaders();
        }

        _rowBuffer = new object[FieldCount];

        for (int i = 0; i < FieldCount; i++)
        {
            if (_columnReaders[i] != null)
            {
                _columnReaders[i].MoveNext();
                _rowBuffer[i] = _columnReaders[i].Current;
            }
        }

        stripeRowIndex++;

        return rowIndex++ < totalAmountOfRecords;
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();

            for (int i = 0; i < FieldCount; i++)
            {
                if (_columnReaders[i] != null)
                {
                    if (_columnReaders[i] is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }

                    _columnReaders[i] = null;
                }
            }
        }
    }

    private IEnumerator GetColumnReader(int fieldIndex)
    {
        var stripeStreamCollection = _fileTail.Stripes[stripeIndex].GetStripeStreamCollection();

        var columnType = _fileTail.Footer.Types[fieldIndex].Kind;
        var uFieldIndex = (uint)fieldIndex;

        switch (columnType)
        {
            case ColumnTypeKind.Long:
            case ColumnTypeKind.Int:
            case ColumnTypeKind.Short:
                return new LongReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Byte:
                return new ByteReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Boolean:
                //return new BooleanReader(stripeStreamCollection, uFieldIndex, _fileTail.Footer.RowIndexStride).Read().GetEnumerator();
                return new BooleanReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Float:
                return new FloatReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Double:
                return new DoubleReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Binary:
                return new BinaryReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Decimal:
                return new DecimalReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Timestamp:
                return new TimestampReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Date:
                return new DateReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.String:
                return new StringReader(stripeStreamCollection, uFieldIndex).Read().GetEnumerator();

            case ColumnTypeKind.Struct:
                return null;
            case ColumnTypeKind.List:
            case ColumnTypeKind.Map:
            case ColumnTypeKind.Union:
            case ColumnTypeKind.Varchar:
            case ColumnTypeKind.Char:
            default:
                throw new NotImplementedException();
        }
    }

    private void InitializeColumnReaders()
    {
        var subTypes = _fileTail.Footer.Types[0].SubTypes
            .Select(s => (int)s)
            .ToArray();

        if (_columnReaders != null)
        {
            for (int i = 0; i < FieldCount; i++)
            {
                if (_columnReaders[i] != null)
                {
                    if (_columnReaders[i] is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }

                    _columnReaders[i] = null;
                }
            }
        }

        _columnReaders = Enumerable.Range(0, FieldCount)
            .Select(i => GetColumnReader(subTypes[i]))
            .ToArray();
    }
}