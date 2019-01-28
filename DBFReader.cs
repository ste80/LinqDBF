/*
 DBFReader
 Class for reading the records assuming that the given
 InputStream comtains DBF data.
 
 This file is part of DotNetDBF packege.
 
 original author (javadbf): anil@linuxense.com 2004/03/31
 
 License: LGPL (http://www.gnu.org/copyleft/lesser.html)
 
 ported to C# (DotNetDBF): Jay Tuley <jay+dotnetdbf@tuley.name> 6/28/2007
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Linq;

namespace LinqDBF
{
    public class DBFReader : DBFBase, IDisposable
    {
        protected Stream _BaseStream { get; set; }
        protected BinaryReader _BinaryRead { get; set; }
        protected DBFHeader _Header { get; set; }
        protected Lazy<Stream> _StreamMemo { get; set; }
        // protected string streamMemoLoc { get; set; }
        protected int[] _SelectedFields = new int[] { };
        protected int[] _OrderedSelectedFields = new int[] { };
        /* Class specific variables */
        protected bool _IsClosed = true;


        /**
		 Returns the number of records in the DBF.
		 */
        public int RecordCount => _Header.NumberOfRecords;

        public Stream DataMemo
        {
            get { return _StreamMemo?.Value; }
            set { _StreamMemo = new Lazy<Stream>(() => value); }
        }

        public Stream BaseStream => _BaseStream;

        /**
		 Returns the asked Field. In case of an invalid index,
		 it returns a ArrayIndexOutofboundsException.
		 
		 @param index. Index of the field. Index of the first field is zero.
		 */
        public DBFField[] Fields => _Header.FieldArray;

        protected static Lazy<Stream> GetMemoStreamByPath(string path, FileShare fileShare)
        {
            return new Lazy<Stream>(() =>
            {
                var dbtPath = Path.ChangeExtension(path, "dbt");
                if (File.Exists(dbtPath))
                {
                    return new FileStream(dbtPath, FileMode.Open, FileAccess.Read, fileShare);
                }
                return null;
            });
        }
        /**
		 Initializes a DBFReader object.
		 
		 When this constructor returns the object
		 will have completed reading the hader (meta date) and
		 header information can be quried there on. And it will
		 be ready to return the first row.
		 
		 @param InputStream where the data is read from.
		 */

        public DBFReader(string path)
            : this(path, FileShare.ReadWrite)
        {
        }

        public DBFReader(string path, FileShare fileShare)
            : this(new FileStream(path, FileMode.Open, FileAccess.Read, fileShare), GetMemoStreamByPath(path, fileShare))
        {
        }

        public DBFReader(Stream streamWithReadAccess, Lazy<Stream> streamForMemo)
        {
            try
            {
                _BaseStream = streamWithReadAccess;
                _BinaryRead = new BinaryReader(_BaseStream);
                _StreamMemo = streamForMemo;

                _IsClosed = false;

                _Header = new DBFHeader();
                _Header.Read(_BinaryRead);

                /* it might be required to leap to the start of records at times */
                var dataStartIndex = _Header.HeaderLength
                                       - (32 + (32 * _Header.FieldArray.Length))
                                       - 1;
                if (dataStartIndex > 0)
                {
                    _BinaryRead.ReadBytes((dataStartIndex));
                }
            }
            catch (IOException e)
            {
                throw new DBFException("Failed To Read DBF", e);
            }
        }

        public void SetSelectFields(params string[] fields)
        {
            _SelectedFields =
                fields.Select(
                    field => Array.FindIndex(_Header.FieldArray, headerField => headerField.Name.Equals(field, StringComparison.OrdinalIgnoreCase))
                ).ToArray();
            _OrderedSelectedFields = _SelectedFields.OrderBy(field => field).ToArray();
        }

        public DBFField[] GetSelectFields()
        {
            return _SelectedFields.Length > 0
                ? _SelectedFields.Select(field => _Header.FieldArray[field]).ToArray()
                : _Header.FieldArray;
        }

        #region IDisposable Members

        /// <summary>Performs application-defined tasks associated with freeing, releasing,
        /// or resetting unmanaged resources.</summary>
        /// <filterpriority>2</filterpriority>
        public virtual void Dispose()
        {
            Close();
        }

        #endregion

        //#if NET35
        //        [Obsolete("Will need to open your own stream and use DataMemo property in later versions of .Net Framework")]
        //        public string DataMemoLoc
        //        {
        //            get { return streamMemoLoc; }
        //            set { streamMemoLoc = value; }
        //        }
        //#endif

        public override String ToString()
        {
            var sb =
                new StringBuilder().Append(_Header.Year.ToString()).Append("/")
                    .Append(_Header.Month.ToString()).Append("/")
                    .AppendLine(_Header.Day.ToString())
                    .Append("Total records: ").AppendLine(_Header.NumberOfRecords.ToString())
                    .Append("Header length: ").Append(_Header.HeaderLength);

            for (var i = 0; i < _Header.FieldArray.Length; i++)
            {
                sb.AppendLine(_Header.FieldArray[i].Name);
            }

            return sb.ToString();
        }

        public virtual void Close()
        {
            //#if NET35
            //            _loadedStream?.Close();
            //            streamMemo?.Close();
            //            _dataInputStream.Close();
            //#else

            if (_StreamMemo.IsValueCreated)
            {
                _StreamMemo.Value?.Dispose();
            }
            //_dataInputStream.Dispose();
            //#endif
            _BaseStream?.Dispose();

            _IsClosed = true;
        }

        /**
		 Reads the returns the next row in the DBF stream.
		 @returns The next row as an Object array. Types of the elements
		 these arrays follow the convention mentioned in the class description.
		 */

        public (Object[], long) NextRecord()
        {
            return NextRecord(_SelectedFields, _OrderedSelectedFields);
        }


        internal (Object[], long) NextRecord(IEnumerable<int> selectIndexes, IList<int> sortedIndexes)
        {
            if (_IsClosed)
            {
                throw new DBFException("Source is not open");
            }

            var position = _BaseStream.Position;
            var tOrderdSelectIndexes = sortedIndexes;
            var recordObjects = new Object[_Header.FieldArray.Length];

            try
            {
                var isDeleted = false;
                do
                {
                    if (isDeleted)
                    {
                        position += _BinaryRead.ReadBytes(_Header.RecordLength - 1).Length;
                    }

                    int t_byte = _BinaryRead.ReadByte();
                    if (t_byte == DBFFieldType.EndOfData)
                    {
                        return (null, position);
                    }

                    position++;
                    isDeleted = (t_byte == '*');
                } while (isDeleted);

                position = position - 1;

                var j = 0;
                var k = -1;
                for (var i = 0; i < _Header.FieldArray.Length; i++)
                {
                    if (tOrderdSelectIndexes.Count == j && j != 0
                        ||
                        (tOrderdSelectIndexes.Count > j && tOrderdSelectIndexes[j] > i && tOrderdSelectIndexes[j] != k))
                    {
                        _BaseStream.Seek(_Header.FieldArray[i].FieldLength, SeekOrigin.Current);
                        continue;
                    }
                    if (tOrderdSelectIndexes.Count > j)
                        k = tOrderdSelectIndexes[j];
                    j++;


                    switch (_Header.FieldArray[i].DataType)
                    {
                        case NativeDbType.UnicodeChar:

                            var b_arrayU = new byte[
                                _Header.FieldArray[i].FieldLength
                                ];
                            _BinaryRead.Read(b_arrayU, 0, b_arrayU.Length);

                            recordObjects[i] = _UCharEncoding.GetString(b_arrayU).TrimEnd('\0', ' ');
                            break;

                        case NativeDbType.Char:

                            var b_array = new byte[
                                _Header.FieldArray[i].FieldLength
                                ];
                            _BinaryRead.Read(b_array, 0, b_array.Length);

                            recordObjects[i] = CharEncoding.GetString(b_array).TrimEnd();
                            break;

                        case NativeDbType.Date:

                            var t_byte_year = new byte[4];
                            _BinaryRead.Read(t_byte_year,
                                0,
                                t_byte_year.Length);

                            var t_byte_month = new byte[2];
                            _BinaryRead.Read(t_byte_month,
                                0,
                                t_byte_month.Length);

                            var t_byte_day = new byte[2];
                            _BinaryRead.Read(t_byte_day,
                                0,
                                t_byte_day.Length);

                            try
                            {
                                var tYear = CharEncoding.GetString(t_byte_year);
                                var tMonth = CharEncoding.GetString(t_byte_month);
                                var tDay = CharEncoding.GetString(t_byte_day);

                                int tIntYear, tIntMonth, tIntDay;
                                if (int.TryParse(tYear, out tIntYear) &&
                                    int.TryParse(tMonth, out tIntMonth) &&
                                    int.TryParse(tDay, out tIntDay))
                                {
                                    recordObjects[i] = new DateTime(
                                        tIntYear,
                                        tIntMonth,
                                        tIntDay);
                                }
                                else
                                {
                                    recordObjects[i] = null;
                                }
                            }
                            catch (ArgumentOutOfRangeException)
                            {
                                /* this field may be empty or may have improper value set */
                                recordObjects[i] = null;
                            }

                            break;

                        case NativeDbType.Float:

                            try
                            {
                                var t_float = new byte[
                                    _Header.FieldArray[i].FieldLength
                                    ];
                                _BinaryRead.Read(t_float, 0, t_float.Length);
                                var tParsed = CharEncoding.GetString(t_float);
                                var tLast = tParsed.Substring(tParsed.Length - 1);
                                if (tParsed.Length > 0
                                    && tLast != " "
                                    && tLast != NullSymbol)
                                {
                                    recordObjects[i] = double.Parse(tParsed,
                                        NumberStyles.Float | NumberStyles.AllowLeadingWhite,
                                        NumberFormatInfo.InvariantInfo);
                                }
                                else
                                {
                                    recordObjects[i] = null;
                                }
                            }
                            catch (FormatException e)
                            {
                                throw new DBFException("Failed to parse Float",
                                    e);
                            }

                            break;

                        case NativeDbType.Numeric:

                            try
                            {
                                var t_numeric = new byte[
                                    _Header.FieldArray[i].FieldLength
                                    ];
                                _BinaryRead.Read(t_numeric,
                                    0,
                                    t_numeric.Length);
                                var tParsed =
                                    CharEncoding.GetString(t_numeric);
                                var tLast = tParsed.Substring(tParsed.Length - 1);
                                if (tParsed.Length > 0
                                    && tLast != " "
                                    && tLast != NullSymbol)
                                {
                                    recordObjects[i] = Decimal.Parse(tParsed,
                                        NumberStyles.Float | NumberStyles.AllowLeadingWhite,
                                        NumberFormatInfo.InvariantInfo);
                                }
                                else
                                {
                                    recordObjects[i] = null;
                                }
                            }
                            catch (FormatException e)
                            {
                                throw new DBFException(
                                    "Failed to parse Number", e);
                            }

                            break;

                        case NativeDbType.Logical:

                            var t_logical = _BinaryRead.ReadByte();
                            //todo find out whats really valid
                            if (t_logical == 'Y' || t_logical == 't'
                                || t_logical == 'T'
                                || t_logical == 't')
                            {
                                recordObjects[i] = true;
                            }
                            else if (t_logical == DBFFieldType.UnknownByte)
                            {
                                recordObjects[i] = DBNull.Value;
                            }
                            else
                            {
                                recordObjects[i] = false;
                            }
                            break;

                        case NativeDbType.Memo:
                            //if (String.IsNullOrEmpty(streamMemoLoc) && streamMemo == null)
                            if (_StreamMemo == null)
                                throw new Exception("Memo Location Not Set");


                            var tRawMemoPointer = _BinaryRead.ReadBytes(_Header.FieldArray[i].FieldLength);
                            var tMemoPoiner = CharEncoding.GetString(tRawMemoPointer);
                            if (string.IsNullOrEmpty(tMemoPoiner))
                            {
                                recordObjects[i] = DBNull.Value;
                                break;
                            }
                            long tBlock;
                            if (!long.TryParse(tMemoPoiner, out tBlock))
                            {
                                //Because Memo files can vary and are often the least importat data, 
                                //we will return null when it doesn't match our format.
                                recordObjects[i] = DBNull.Value;
                                break;
                            }


                            //recordObjects[i] = new MemoValue(tBlock, this, streamMemoLoc, GetLazyStreamFromLocation());
                            recordObjects[i] = new MemoValue(tBlock, this, _StreamMemo);
                            break;
                        default:
                            _BinaryRead.ReadBytes(_Header.FieldArray[i].FieldLength);
                            recordObjects[i] = DBNull.Value;
                            break;
                    }
                }
            }
            catch (EndOfStreamException)
            {
                return (null, position);
            }
            catch (IOException e)
            {
                throw new DBFException("Problem Reading File", e);
            }

            return (selectIndexes.Any() ? selectIndexes.Select(it => recordObjects[it]).ToArray() : recordObjects, position);
        }

        public IEnumerable<DBFRecord> AsEnumerable()
        {
            return AsEnumerable(null);
        }

        public IEnumerable<DBFRecord> AsEnumerable(Func<DBFRecord, bool> filter)
        {
            var lookupFieldName = GetSelectFields()
                .Select((f, i) => new { f.Name, Index = i })
                .ToDictionary(f => f.Name, f => f.Index);

            var (objects, position) = NextRecord();

            while (objects != null)
            {
                var record = new DBFRecord(lookupFieldName, objects, position);
                if (record != null || filter(record) == true)
                {
                    yield return record;
                }

                (objects, position) = NextRecord();
            }
        }
    }
}
