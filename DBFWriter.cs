/*
 DBFWriter
 Class for defining a DBF structure and addin data to that structure and
 finally writing it to an OutputStream.
 
 This file is part of DotNetDBF packege.
 
 original author (javadbf): anil@linuxense.com 2004/03/31
 
 license: LGPL (http://www.gnu.org/copyleft/lesser.html)
 
 ported to C# (DotNetDBF): Jay Tuley <jay+dotnetdbf@tuley.name> 6/28/2007
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace LinqDBF
{
    //public class DBFWriter : DBFBase, IDisposable
    public class DBFWriter : DBFReader
    {
        protected BinaryWriter _BinaryWriter { get; set; }
        //private DBFHeader header;
        //private Stream writeStream;
        //private int recordCount;
        //private List<object> v_records = new List<object>();
        //private string _dataMemoLoc;
        //private Stream _dataMemo;
        ////private string _nullSymbol;

        ///// Creates an empty object.
        //public DBFWriter()
        //{
        //    header = new DBFHeader();
        //}

        //#if NET35


        //        /// Creates a DBFWriter which can append to records to an existing DBF file.
        //        /// @param dbfFile. The file passed in shouls be a valid DBF file.
        //        /// @exception Throws DBFException if the passed in file does exist but not a valid DBF file, or if an IO error occurs.
        //        public DBFWriter(String dbfFile)
        //        {
        //            try
        //            {
        //                raf =
        //                    File.Open(dbfFile,
        //                        FileMode.OpenOrCreate,
        //                        FileAccess.ReadWrite);

        //                DataMemoLoc = Path.ChangeExtension(dbfFile, "dbt");

        //                /* before proceeding check whether the passed in File object
        //				 is an empty/non-existent file or not.
        //				 */
        //                if (raf.Length == 0)
        //                {
        //                    header = new DBFHeader();
        //                    return;
        //                }

        //                header = new DBFHeader();
        //                header.Read(new BinaryReader(raf));

        //                /* position file pointer at the end of the raf */
        //                raf.Seek(-1, SeekOrigin.End);
        //                /* to ignore the END_OF_DATA byte at EoF */
        //            }
        //            catch (FileNotFoundException e)
        //            {
        //                throw new DBFException("Specified file is not found. ", e);
        //            }
        //            catch (IOException e)
        //            {
        //                throw new DBFException(" while reading header", e);
        //            }
        //            recordCount = header.NumberOfRecords;
        //        }
        //#endif

        public DBFWriter(string path)
            : this(path, FileShare.Read)
        {
        }

        public DBFWriter(string path, FileShare fileShare)
            : this(File.Open(path, FileMode.Open, FileAccess.ReadWrite, fileShare), GetMemoStreamByPath(path, fileShare))
        {
        }

        public DBFWriter(Stream streamWithReadWriteAcces, Lazy<Stream> streamForMemo)
            : base(streamWithReadWriteAcces, streamForMemo)
        {
            //writeStream = streamWithWriteAcces;
            _BinaryWriter = new BinaryWriter(streamWithReadWriteAcces);

            //        header = new DBFHeader();

            //        /* before proceeding check whether the passed in File object
            //is an empty/non-existent file or not.
            //*/
            //        if (writeStream.Length == 0)
            //        {
            //            return;
            //        }

            //        header.Read(new BinaryReader(writeStream));

            //        /* position file pointer at the end of the raf */
            //        writeStream.Seek(-1, SeekOrigin.End);
            /* to ignore the END_OF_DATA byte at EoF */


            //recordCount = header.NumberOfRecords;
        }

        public byte Signature
        {
            get { return _Header.Signature; }
            set { _Header.Signature = value; }
        }

        ////#if NET35

        //        public string DataMemoLoc
        //        {
        //            get { return _dataMemoLoc; }
        //            set
        //            {
        //                _dataMemoLoc = value;

        //                _dataMemo?.Close();
        //                _dataMemo = File.Open(_dataMemoLoc,
        //                    FileMode.OpenOrCreate,
        //                    FileAccess.ReadWrite);
        //            }
        //        }
        ////#endif

        //public Stream DataMemo
        //{
        //    get { return _dataMemo; }
        //    set { _dataMemo = value; }
        //}

        public byte LanguageDriver
        {
            set
            {
                if (_Header.LanguageDriver != 0x00)
                {
                    throw new DBFException("LanguageDriver has already been set");
                }

                _Header.LanguageDriver = value;
            }
        }


        // public DBFField[] Fields
        // {
        //     get { return header.FieldArray; }


        //     set
        //     {
        //         if (header.FieldArray != null)
        //         {
        //             throw new DBFException("Fields has already been set");
        //         }

        //         if (value == null || value.Length == 0)
        //         {
        //             throw new DBFException("Should have at least one field");
        //         }

        //         for (var i = 0; i < value.Length; i++)
        //         {
        //             if (value[i] == null)
        //             {
        //                 throw new DBFException("Field " + (i + 1) + " is null");
        //             }
        //         }

        //         header.FieldArray = value;

        //         try
        //         {
        //             //if (writeStream != null && writeStream.Length == 0)
        //             if (_BaseStream?.Length == 0)
        //             {
        //                 /*
        //this is a new/non-existent file. So write header before proceeding
        //*/
        //                 //header.Write(new BinaryWriter(writeStream));
        //                 header.Write(binaryWriter);
        //             }
        //         }
        //         catch (IOException e)
        //         {
        //             throw new DBFException("Error accesing file", e);
        //         }
        //     }
        // }

        //#region IDisposable Members

        ///// <summary>Performs application-defined tasks associated with freeing, releasing,
        ///// or resetting unmanaged resources.</summary>
        ///// <filterpriority>2</filterpriority>
        //public override void Dispose()
        //{
        //    Close();
        //}

        //#endregion

        /**
		 Add a record.
		 */

        public void WriteRecord(params object[] values)
        {
            //if (writeStream == null)
            //{
            //    throw new DBFException(
            //        "Not initialized with file for WriteRecord use, use AddRecord instead");
            //}
            //AddRecord(values, true);
            //AddRecord(values);

            Validate(values);

            //if (!writeImediately)
            //{
            //    v_records.Add(values);
            //}
            //else
            //{
            try
            {
                //WriteRecord(new BinaryWriter(writeStream), values);
                WriteRecord(_BinaryWriter, values);
                //recordCount++;
            }
            catch (IOException e)
            {
                throw new DBFException(
                    "Error occured while writing record. ", e);
            }
            //}
        }

        public void WriteRecord(DBFRecord record)
        {
            if (!record.IsDirty())
            {
                return;
            }
            //if (writeStream == null)
            //{
            //    throw new DBFException(
            //        "Not initialized with file for WriteRecord use, use AddRecord instead");
            //}
            //AddRecord(values, true);
            //AddRecord(record.ValueArray);

            Validate(record.ValueArray);

            //if (!writeImediately)
            //{
            //    v_records.Add(values);
            //}
            //else
            //{
            try
            {
                //WriteRecord(new BinaryWriter(writeStream), values);
                WriteRecord(_BinaryWriter, record);
                //recordCount++;
            }
            catch (IOException e)
            {
                throw new DBFException(
                    "Error occured while writing record. ", e);
            }
            //}
        }

        //public void AddRecord(params object[] values)
        //{
        //    if (writeStream != null)
        //    {
        //        throw new DBFException(
        //            "Appending to a file, requires using Writerecord instead");
        //    }
        //    AddRecord(values, false);
        //}

        private void Validate(object[] values)
        {
            if (_Header.FieldArray == null)
            {
                throw new DBFException(
                    "Fields should be set before adding records");
            }

            if (values == null)
            {
                throw new DBFException("Null cannot be added as row");
            }

            if (values.Length
                != _Header.FieldArray.Length)
            {
                throw new DBFException(
                    "Invalid record. Invalid number of fields in row");
            }

            for (var i = 0; i < _Header.FieldArray.Length; i++)
            {
                if (values[i] == null)
                {
                    continue;
                }

                switch (_Header.FieldArray[i].DataType)
                {
                    case NativeDbType.UnicodeChar:
                        if (!(values[i] is String) && !(values[i] is DBNull))
                        {
                            throw new DBFRecordException($"Invalid value for field {i}", i);
                        }
                        break;

                    case NativeDbType.Char:
                        if (!(values[i] is String) && !(values[i] is DBNull))
                        {
                            throw new DBFRecordException($"Invalid value for field {i}", i);
                        }
                        break;

                    case NativeDbType.Logical:
                        if (!(values[i] is Boolean) && !(values[i] is DBNull))
                        {
                            throw new DBFRecordException($"Invalid value for field {i}", i);
                        }
                        break;

                    case NativeDbType.Numeric:
                        if (!(values[i] is IConvertible) && !(values[i] is DBNull))
                        {
                            throw new DBFRecordException($"Invalid value for field {i}", i);
                        }
                        break;

                    case NativeDbType.Date:
                        if (!(values[i] is DateTime) && !(values[i] is DBNull))
                        {
                            throw new DBFRecordException($"Invalid value for field {i}", i);
                        }
                        break;

                    case NativeDbType.Float:
                        if (!(values[i] is IConvertible) && !(values[i] is DBNull))
                        {
                            throw new DBFRecordException($"Invalid value for field {i}", i);
                        }
                        break;
                    case NativeDbType.Memo:
                        if (!(values[i] is MemoValue) && !(values[i] is DBNull))
                        {
                            throw new DBFRecordException($"Invalid value for field {i}", i);
                        }
                        break;
                }
            }
        }

        ////private void AddRecord(object[] values, bool writeImediately)
        //private void AddRecord(object[] values)
        //{
        //    Validate(values);

        //    //if (!writeImediately)
        //    //{
        //    //    v_records.Add(values);
        //    //}
        //    //else
        //    //{
        //    try
        //    {
        //        //WriteRecord(new BinaryWriter(writeStream), values);
        //        WriteRecord(binaryWriter, values);
        //        //recordCount++;
        //        header.NumberOfRecords++;
        //    }
        //    catch (IOException e)
        //    {
        //        throw new DBFException(
        //            "Error occured while writing record. ", e);
        //    }
        //    //}
        //}

        /////Writes the set data to the OutputStream.
        //public void Write(Stream tOut)
        //{
        //    try
        //    {
        //        var outStream = new BinaryWriter(tOut);
        //        header.NumberOfRecords = v_records.Count;
        //        header.Write(outStream);

        //        /* Now write all the records */
        //        var t_recCount = v_records.Count;
        //        for (var i = 0; i < t_recCount; i++)
        //        {
        //            /* iterate through records */

        //            var t_values = (object[])v_records[i];

        //            WriteRecord(outStream, t_values);
        //        }

        //        outStream.Write(DBFFieldType.EndOfData);
        //        outStream.Flush();
        //    }
        //    catch (IOException e)
        //    {
        //        throw new DBFException("Error Writing", e);
        //    }
        //}

        public override void Close()
        {
            //header.NumberOfRecords = recordCount;
            //if (writeStream != null)
            //{
            //writeStream.Seek(0, SeekOrigin.Begin);
            _BinaryWriter.Seek(0, SeekOrigin.Begin);
            //header.Write(new BinaryWriter(writeStream));
            _Header.Write(_BinaryWriter);
            ///* everything is written already. just update the header for record count and the END_OF_DATA mark */
            ////header.NumberOfRecords = recordCount;
            ////if (writeStream != null)
            ////{
            ////writeStream.Seek(0, SeekOrigin.Begin);
            //binaryWriter.Seek(0, SeekOrigin.Begin);
            ////header.Write(new BinaryWriter(writeStream));
            //header.Write(binaryWriter);
            ////writeStream.Seek(0, SeekOrigin.End);
            //binaryWriter.Seek(0, SeekOrigin.Begin);
            ////writeStream.WriteByte(DBFFieldType.EndOfData);
            //binaryWriter.Seek(0, SeekOrigin.End);
            //binaryWriter.Write(DBFFieldType.EndOfData);
            ////#if NET35
            //                raf.Close();
            //                _dataMemo?.Close();
            ////#else
            base.Close();
            ////writeStream.Dispose();
            //_BaseStream.Dispose();
            ////_dataMemo?.Dispose();
            //DataMemo?.Dispose();
            //#endif
            //}

            //#if NET35


            //            if (!String.IsNullOrEmpty(DataMemoLoc))
            //            {
            //                DataMemo.Close();
            //            }
            //#endif
        }

        public void WriteRecords(params DBFRecord[] values)
        {
            //if (writeStream == null)
            //{
            //    throw new DBFException(
            //        "Not initialized with file for WriteRecord use, use AddRecord instead");
            //}

            if (values == null || values.Length <= 0)
            {
                return;
            }

            foreach (DBFRecord record in values)
            {
                Validate(record.ValueArray);
            }


            //var outStream = new BinaryWriter(writeStream);

            foreach (DBFRecord record in values)
            {
                //writeStream.Seek(record.Position, SeekOrigin.Begin);
                _BaseStream.Seek(record.Position, SeekOrigin.Begin);

                if (record.IsDelete())
                    //outStream.Write((byte)'*');
                    _BinaryWriter.Write((byte)'*');
                else
                    //WriteRecord(outStream, record.ValueArray);
                    WriteRecord();
            }

            //outStream.Flush();
            _BinaryWriter.Flush();

        }

        public void AppendRecords(params object[][] values)
        {
            //if (writeStream == null)
            //{
            //    throw new DBFException(
            //        "Not initialized with file for WriteRecord use, use AddRecord instead");
            //}

            if (values == null || values.Length <= 0)
            {
                return;
            }

            foreach (object[] record in values)
            {
                Validate(record);
            }

            try
            {
                //var outStream = new BinaryWriter(writeStream);

                /* Now write all the records */
                var pos = 0;

                _BaseStream.Seek(pos - 1, SeekOrigin.End);
                var peekChar = _BinaryRead.PeekChar();
                while (peekChar == DBFFieldType.EndOfData)
                {
                    _BaseStream.Seek(--pos - 1, SeekOrigin.End);
                    peekChar = _BinaryRead.PeekChar();
                }

                _BaseStream.Seek(pos, SeekOrigin.End);
                //var pos = _BaseStream.Position;
                //int end = _BinaryRead.PeekChar();
                var t_recCount = values.Length;
                for (var i = 0; i < t_recCount; i++)
                {
                    /* iterate through records */
                    var t_values = (object[])values[i];

                    //WriteRecord(outStream, t_values);
                    WriteRecord(t_values);
                    _Header.NumberOfRecords++;
                }

                //outStream.Write(DBFFieldType.EndOfData);
                _BinaryWriter.Write(DBFFieldType.EndOfData);
                //outStream.Flush();
                _BinaryWriter.Flush();
            }
            catch (IOException e)
            {
                throw new DBFException("Error Writing", e);
            }
        }

        //private void WriteRecord(BinaryWriter dataOutput, object[] objectArray)
        private void WriteRecord(BinaryWriter dataOutput, object[] objectArray)
        {
            dataOutput.Write((byte)' ');
            for (var j = 0; j < _Header.FieldArray.Length; j++)
            {
                /* iterate throught fields */

                switch (_Header.FieldArray[j].DataType)
                {
                    case NativeDbType.UnicodeChar:
                        var strValueU = (objectArray[j] != null && objectArray[j] != DBNull.Value) ? objectArray[j].ToString() : "";
                        var buffer = Utils.textPadding(strValueU,
                                _UCharEncoding,
                                _Header.FieldArray[j].FieldLength,
                                Utils.ALIGN_LEFT,
                                (byte)0x00
                            );
                        dataOutput.Write(buffer);
                        break;

                    case NativeDbType.Char:
                        var str_value = (objectArray[j] != null && objectArray[j] != DBNull.Value) ? objectArray[j].ToString() : "";
                        dataOutput.Write(
                            Utils.textPadding(str_value,
                                CharEncoding,
                                _Header.FieldArray[j].FieldLength
                            )
                        );
                        break;

                    case NativeDbType.Date:
                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            var tDate = (DateTime)objectArray[j];

                            dataOutput.Write(
                                CharEncoding.GetBytes(tDate.ToString("yyyyMMdd")));
                        }
                        else
                        {
                            dataOutput.Write(
                                Utils.FillArray(new byte[8], DBFFieldType.Space));
                        }

                        break;

                    case NativeDbType.Float:

                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            var tDouble = Convert.ToDouble(objectArray[j]);
                            dataOutput.Write(
                                Utils.NumericFormating(
                                    tDouble,
                                    CharEncoding,
                                    _Header.FieldArray[j].FieldLength,
                                    _Header.FieldArray[j].DecimalCount
                                )
                            );
                        }
                        else
                        {
                            dataOutput.Write(
                                Utils.textPadding(
                                    NullSymbol,
                                    CharEncoding,
                                    _Header.FieldArray[j].FieldLength,
                                    Utils.ALIGN_RIGHT
                                )
                            );
                        }

                        break;

                    case NativeDbType.Numeric:

                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            var tDecimal = Convert.ToDecimal(objectArray[j]);
                            dataOutput.Write(
                                Utils.NumericFormating(
                                    tDecimal,
                                    CharEncoding,
                                    _Header.FieldArray[j].FieldLength,
                                    _Header.FieldArray[j].DecimalCount
                                )
                            );
                        }
                        else
                        {
                            dataOutput.Write(
                                Utils.textPadding(
                                    NullSymbol,
                                    CharEncoding,
                                    _Header.FieldArray[j].FieldLength,
                                    Utils.ALIGN_RIGHT
                                )
                            );
                        }

                        break;
                    case NativeDbType.Logical:

                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            if ((bool)objectArray[j])
                            {
                                dataOutput.Write(DBFFieldType.True);
                            }
                            else
                            {
                                dataOutput.Write(DBFFieldType.False);
                            }
                        }
                        else
                        {
                            dataOutput.Write(DBFFieldType.UnknownByte);
                        }

                        break;

                    case NativeDbType.Memo:
                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            var tMemoValue = ((MemoValue)objectArray[j]);

                            tMemoValue.Write(this);

                            dataOutput.Write(Utils.NumericFormating(tMemoValue.Block, CharEncoding, 10, 0));
                        }
                        else
                        {
                            dataOutput.Write(
                                Utils.textPadding("",
                                    CharEncoding,
                                    10
                                )
                            );
                        }


                        break;

                    default:
                        throw new DBFException("Unknown field type "
                                               + _Header.FieldArray[j].DataType);
                }
            } /* iterating through the fields */
        }

        private void WriteRecord(BinaryWriter dataOutput, DBFRecord record)
        {
            if (!record.IsDirty())
            {
                return;
            }

            //var originalPosition = _BaseStream.Position;
            _BaseStream.Seek(record.Position, SeekOrigin.Begin);
            dataOutput.Write((byte)' ');

            for (var j = 0; j < _Header.FieldArray.Length; j++)
            {
                /* iterate throught fields */
                if (!record.IsDirty(j))
                {
                    _BaseStream.Seek(_Header.FieldArray[j].FieldLength, SeekOrigin.Current);
                    continue;
                }

                var objectArray = record.ValueArray;

                switch (_Header.FieldArray[j].DataType)
                {
                    case NativeDbType.UnicodeChar:
                        var str_valueU = (objectArray[j] != null && objectArray[j] != DBNull.Value) ? objectArray[j].ToString() : "";
                        var bufferU = Utils.textPadding(str_valueU,
                            _UCharEncoding,
                            _Header.FieldArray[j].FieldLength,
                            Utils.ALIGN_LEFT,
                            (byte)0x00
                        );
                        dataOutput.Write(bufferU);
                        break;

                    case NativeDbType.Char:
                        var str_value = (objectArray[j] != null && objectArray[j] != DBNull.Value) ? objectArray[j].ToString() : "";
                        var buffer = Utils.textPadding(str_value,
                            CharEncoding,
                            _Header.FieldArray[j].FieldLength
                        );
                        dataOutput.Write(buffer);
                        break;

                    case NativeDbType.Date:
                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            var tDate = (DateTime)objectArray[j];

                            dataOutput.Write(
                                CharEncoding.GetBytes(tDate.ToString("yyyyMMdd")));
                        }
                        else
                        {
                            dataOutput.Write(
                                Utils.FillArray(new byte[8], DBFFieldType.Space));
                        }

                        break;

                    case NativeDbType.Float:

                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            var tDouble = Convert.ToDouble(objectArray[j]);
                            dataOutput.Write(
                                Utils.NumericFormating(
                                    tDouble,
                                    CharEncoding,
                                    _Header.FieldArray[j].FieldLength,
                                    _Header.FieldArray[j].DecimalCount
                                )
                            );
                        }
                        else
                        {
                            dataOutput.Write(
                                Utils.textPadding(
                                    NullSymbol,
                                    CharEncoding,
                                    _Header.FieldArray[j].FieldLength,
                                    Utils.ALIGN_RIGHT
                                )
                            );
                        }

                        break;

                    case NativeDbType.Numeric:

                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            var tDecimal = Convert.ToDecimal(objectArray[j]);
                            dataOutput.Write(
                                Utils.NumericFormating(
                                    tDecimal,
                                    CharEncoding,
                                    _Header.FieldArray[j].FieldLength,
                                    _Header.FieldArray[j].DecimalCount
                                )
                            );
                        }
                        else
                        {
                            dataOutput.Write(
                                Utils.textPadding(
                                    NullSymbol,
                                    CharEncoding,
                                    _Header.FieldArray[j].FieldLength,
                                    Utils.ALIGN_RIGHT
                                )
                            );
                        }

                        break;
                    case NativeDbType.Logical:

                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            if ((bool)objectArray[j])
                            {
                                dataOutput.Write(DBFFieldType.True);
                            }
                            else
                            {
                                dataOutput.Write(DBFFieldType.False);
                            }
                        }
                        else
                        {
                            dataOutput.Write(DBFFieldType.UnknownByte);
                        }

                        break;

                    case NativeDbType.Memo:
                        if (objectArray[j] != null && objectArray[j] != DBNull.Value)
                        {
                            var tMemoValue = ((MemoValue)objectArray[j]);

                            tMemoValue.Write(this);

                            dataOutput.Write(Utils.NumericFormating(tMemoValue.Block, CharEncoding, 10, 0));
                        }
                        else
                        {
                            dataOutput.Write(
                                Utils.textPadding("",
                                    CharEncoding,
                                    10
                                )
                            );
                        }


                        break;

                    default:
                        throw new DBFException("Unknown field type "
                                               + _Header.FieldArray[j].DataType);
                }
            } /* iterating through the fields */
            _BinaryWriter.Flush();
            //_BaseStream.Seek(originalPosition, SeekOrigin.Begin);
        }


        //public int Upsert(DBFReader reader, Func<DBFRecord, bool> filter = null, Func<DBFRecord, DBFRecord> updater = null, Func<IEnumerable<object[]>> inserter = null)
        //{
        //    int countUpdate = 0;

        //    foreach (DBFRecord record in reader.AsEnumerable())
        //    {
        //        if (filter == null || filter(record))
        //        {
        //            var updateResult = updater(record);
        //            if (updateResult != null)
        //            {
        //                if (updateResult.Deleted == true)
        //                {
        //                    raf.Seek(record.Position, SeekOrigin.Begin);
        //                    new BinaryWriter(raf).Write((byte) '*');
        //                }
        //                else if (updateResult.Dirty == true)
        //                {
        //                    raf.Seek(record.Position, SeekOrigin.Begin);
        //                    WriteRecord(updateResult.ValueArray);
        //                }
        //            }
        //        }
        //    }

        //    if (inserter != null)
        //    {
        //        var insertResult = inserter();
        //        if (insertResult != null)
        //        {
        //            foreach (var record in insertResult)
        //            {
        //                WriteRecord(record);
        //            }
        //        }
        //    }

        //    return countUpdate;
        //}
    }
}
