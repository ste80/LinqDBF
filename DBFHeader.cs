/*
 DBFHeader
 Class for reading the metadata assuming that the given
 InputStream carries DBF data.
 
 This file is part of DotNetDBF packege.
 
 original author (javadbf): anil@linuxense.com 2004/03/31
 
 License: LGPL (http://www.gnu.org/copyleft/lesser.html)
 
 ported to C# (DotNetDBF): Jay Tuley <jay+dotnetdbf@tuley.name> 6/28/2007
 

 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace LinqDBF
{
    public static class DBFSigniture
    {
        public const byte NotSet = 0,
            WithMemo = 0x80,
            DBase3 = 0x03,
            DBase3WithMemo = DBase3 | WithMemo;
    }

    [Flags]
    public enum MemoFlags : byte
    {
    }


    public class DBFHeader
    {
        private byte _Signature; /* 0 */
        private byte _Year; /* 1 */
        private byte _Month; /* 2 */
        private byte _Day; /* 3 */
        private int _NumberOfRecords; /* 4-7 */
        private short _HheaderLength; /* 8-9 */
        private short _RecordLength; /* 10-11 */
        private short _Reserv1; /* 12-13 */
        private byte _IncompleteTransaction; /* 14 */
        private byte _EncryptionFlag; /* 15 */
        private int _FreeRecordThread; /* 16-19 */
        private int _Reserv2; /* 20-23 */
        private int _Reserv3; /* 24-27 */
        private byte _MdxFlag; /* 28 */
        private byte _LanguageDriver; /* 29 */
        private short Reserv4; /* 30-31 */
        private DBFField[] _FieldArray; /* each 32 bytes */

        internal byte Signature
        {
            get { return _Signature; }
            set { _Signature = value; }
        }

        internal short Size => (short)(sizeof(byte) +
                                        sizeof(byte) + sizeof(byte) + sizeof(byte) +
                                        sizeof(int) +
                                        sizeof(short) +
                                        sizeof(short) +
                                        sizeof(short) +
                                        sizeof(byte) +
                                        sizeof(byte) +
                                        sizeof(int) +
                                        sizeof(int) +
                                        sizeof(int) +
                                        sizeof(byte) +
                                        sizeof(byte) +
                                        sizeof(short) +
                                        (DBFField.SIZE * _FieldArray.Length) +
                                        sizeof(byte));

        internal short RecordSize
        {
            get
            {
                var tRecordLength = 0;
                for (var i = 0; i < _FieldArray.Length; i++)
                {
                    tRecordLength += _FieldArray[i].FieldLength;
                }

                return (short)(tRecordLength + 1);
            }
        }

        internal short HeaderLength
        {
            set { _HheaderLength = value; }

            get { return _HheaderLength; }
        }

        internal DBFField[] FieldArray
        {
            set { _FieldArray = value; }

            get { return _FieldArray; }
        }

        internal byte Year
        {
            set { _Year = value; }

            get { return _Year; }
        }

        internal byte Month
        {
            set { _Month = value; }

            get { return _Month; }
        }

        internal byte Day
        {
            set { _Day = value; }

            get { return _Day; }
        }

        internal int NumberOfRecords
        {
            set { _NumberOfRecords = value; }

            get { return _NumberOfRecords; }
        }

        internal short RecordLength
        {
            set { _RecordLength = value; }

            get { return _RecordLength; }
        }

        internal byte LanguageDriver
        {
            get { return _LanguageDriver; }
            set { _LanguageDriver = value; }
        }
        
        public const byte HeaderRecordTerminator = 0x0D;
        
        public DBFHeader()
        {
            _Signature = DBFSigniture.DBase3;
        }

        internal void Read(BinaryReader dataInput)
        {
            _Signature = dataInput.ReadByte(); /* 0 */
            /* 1 byte
File type:
0x02   FoxBASE
0x03   FoxBASE+/Dbase III plus, no memo
0x30   Visual FoxPro
0x31   Visual FoxPro, autoincrement enabled
0x32   Visual FoxPro, Varchar, Varbinary, or Blob-enabled
0x43   dBASE IV SQL table files, no memo
0x63   dBASE IV SQL system files, no memo
0x83   FoxBASE+/dBASE III PLUS, with memo
0x8B   dBASE IV with memo
0xCB   dBASE IV SQL table files, with memo
0xF5   FoxPro 2.x (or earlier) with memo
0xFB   FoxBASE */

            _Year = dataInput.ReadByte(); /* 1 */
            _Month = dataInput.ReadByte(); /* 2 */
            _Day = dataInput.ReadByte(); /* 3 */
            /* 3 bytes
Last update (YYMMDD) */

            _NumberOfRecords = dataInput.ReadInt32(); /* 4-7 */
            /* 3 bytes
Number of records in file */

            _HheaderLength = dataInput.ReadInt16(); /* 8-9 */
            /* 2 bytes
Position of first data record */

            _RecordLength = dataInput.ReadInt16(); /* 10-11 */
            /* 2 bytes
Length of one data record, including delete flag */

            _Reserv1 = dataInput.ReadInt16(); /* 12-13 */
            /* 2 bytes
Reserved */
            _IncompleteTransaction = dataInput.ReadByte(); /* 14 */
            /* 1 byte
Incomplete transaction/Transaction ended (or rolled back)/Transaction started */

            _EncryptionFlag = dataInput.ReadByte(); /* 15 */
            /* 1 byte
Encryption flag */

            _FreeRecordThread = dataInput.ReadInt32(); /* 16-19 */
            _Reserv2 = dataInput.ReadInt32(); /* 20-23 */
            _Reserv3 = dataInput.ReadInt32(); /* 24-27 */
            /* 12 bytes
Reserved */

            _MdxFlag = dataInput.ReadByte(); /* 28 */
            /* 1 byte
Table flags:
0x01   file has a structural .cdx
0x02   file has a Memo field
0x04   file is a database (.dbc)
This byte can contain the sum of any of the above values. For example, the value 0x03 indicates the table has a structural .cdx and a Memo field. */
            _LanguageDriver = dataInput.ReadByte(); /* 29 */
            /* 1 byte
Code page mark */
            Reserv4 = dataInput.ReadInt16(); /* 30-31 */
            /* 2 bytes
Reserved, contains 0x00 */

            var v_fields = new List<DBFField>();

            var field = DBFField.CreateField(dataInput); /* 32 each */
            while (field != null)
            {
                v_fields.Add(field);
                field = DBFField.CreateField(dataInput);
            }

            _FieldArray = v_fields.ToArray();
            //System.out.println( "Number of fields: " + _fieldArray.length);
        }

        internal void Write(BinaryWriter dataOutput)
        {   
            //System.out.println( "Number of fields: " + _fieldArray.length);
            

            dataOutput.Write(_Signature); /* 0 */
            /* 1 byte
File type:
0x02   FoxBASE
0x03   FoxBASE+/Dbase III plus, no memo
0x30   Visual FoxPro
0x31   Visual FoxPro, autoincrement enabled
0x32   Visual FoxPro, Varchar, Varbinary, or Blob-enabled
0x43   dBASE IV SQL table files, no memo
0x63   dBASE IV SQL system files, no memo
0x83   FoxBASE+/dBASE III PLUS, with memo
0x8B   dBASE IV with memo
0xCB   dBASE IV SQL table files, with memo
0xF5   FoxPro 2.x (or earlier) with memo
0xFB   FoxBASE */

            var tNow = DateTime.Now;
            _Year = (byte)(tNow.Year - 1900);
            _Month = (byte)(tNow.Month);
            _Day = (byte)(tNow.Day);
            
            dataOutput.Write(_Year); /* 1 */
            dataOutput.Write(_Month); /* 2 */
            dataOutput.Write(_Day); /* 3 */
            /* 3 bytes
Last update (YYMMDD) */

            //System.out.println( "Number of records in O/S: " + numberOfRecords);
            dataOutput.Write(_NumberOfRecords); /* 4-7 */
            /* 3 bytes
Number of records in file */

            //_headerLength = Size;
            dataOutput.Write(_HheaderLength); /* 8-9 */
            /* 2 bytes
Position of first data record */

            _RecordLength = RecordSize;
            dataOutput.Write(_RecordLength); /* 10-11 */
            /* 2 bytes
Length of one data record, including delete flag */

            dataOutput.Write(_Reserv1); /* 12-13 */
            /* 2 bytes
Reserved */

            dataOutput.Write(_IncompleteTransaction); /* 14 */
            /* 1 byte
Incomplete transaction/Transaction ended (or rolled back)/Transaction started */

            dataOutput.Write(_EncryptionFlag); /* 15 */
            /* 1 byte
Encryption flag */

            dataOutput.Write(_FreeRecordThread); /* 16-19 */
            dataOutput.Write(_Reserv2); /* 20-23 */
            dataOutput.Write(_Reserv3); /* 24-27 */
            /* 12 bytes
Reserved */

            dataOutput.Write(_MdxFlag); /* 28 */
            /* 1 byte
Table flags:
0x01   file has a structural .cdx
0x02   file has a Memo field
0x04   file is a database (.dbc)
This byte can contain the sum of any of the above values. For example, the value 0x03 indicates the table has a structural .cdx and a Memo field. */
            dataOutput.Write(_LanguageDriver == 0 ? (byte)0x03 : _LanguageDriver); /* 29 */
            /* 1 byte
Code page mark */
            dataOutput.Write(Reserv4); /* 30-31 */
            /* 2 bytes
Reserved, contains 0x00 */

            for (var i = 0; i < _FieldArray.Length; i++)
            {
                //System.out.println( "Length: " + _fieldArray[i].getFieldLength());
                _FieldArray[i].Write(dataOutput);
            }

            dataOutput.Write(HeaderRecordTerminator); /* n+1 */
        }
    }
}