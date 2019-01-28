/*
 DBFField
 Class represents a "field" (or column) definition of a DBF data structure.
 
 This file is part of DotNetDBF packege.
 
 original author (javadbf): anil@linuxense.com 2004/03/31
 
 license: LGPL (http://www.gnu.org/copyleft/lesser.html)
 
 ported to C# (DotNetDBF): Jay Tuley <jay+dotnetdbf@tuley.name> 6/28/2007
 

 */

using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace LinqDBF
{
    [DebuggerDisplay("Field:{Name}, Length:{FieldLength}")]
    public class DBFField
    {
        public const int SIZE = 32;
        public byte dataType; /* 11 */
        public byte decimalCount; /* 17 */
        public int fieldLength; /* 16 */
        public byte[] fieldName = new byte[11]; /* 0-10*/
        public byte indexFieldFlag; /* 31 */

        /* other class variables */
        public int nameNullIndex = 0;
        public int reserv1; /* 12-15 */
        public short reserv2; /* 18-19 */
        public short reserv3; /* 21-22 */
        public byte[] reserv4 = new byte[7]; /* 24-30 */
        public byte setFieldsFlag; /* 23 */
        public byte workAreaId; /* 20 */

        public DBFField()
        {
        }

        public DBFField(string aFieldName, NativeDbType aType)
        {
            Name = aFieldName;
            DataType = aType;
        }

        public DBFField(string aFieldName,
                        NativeDbType aType,
                        int aFieldLength)
        {
            Name = aFieldName;
            DataType = aType;
            FieldLength = aFieldLength;
        }

        public DBFField(string aFieldName,
                        NativeDbType aType,
                        int aFieldLength,
                        int aDecimalCount)
        {
            Name = aFieldName;
            DataType = aType;
            FieldLength = aFieldLength;
            DecimalCount = aDecimalCount;
        }

        public int Size => SIZE;

        /**
         Returns the name of the field.
         
         @return Name of the field as string.
         */

        public string Name
        {
            get => Encoding.ASCII.GetString(fieldName, 0, nameNullIndex);
            set
            {
                if (value == null)
                {
                    throw new ArgumentException("Field name cannot be null");
                }

                if (value.Length == 0
                    || value.Length > 10)
                {
                    throw new ArgumentException(
                        "Field name should be of length 0-10");
                }

                fieldName = Encoding.ASCII.GetBytes(value);
                nameNullIndex = fieldName.Length;
            }
        }

        /**
         Returns the data type of the field.
         
         @return Data type as byte.
         */

        public Type Type => Utils.TypeForNativeDBType(DataType);


        public NativeDbType DataType
        {
            get
            {
                return (NativeDbType)dataType;
            }
            set
            {
                switch (value)
                {
                    case NativeDbType.Date:
                        fieldLength = 8; /* fall through */
                        goto default;
                    case NativeDbType.Memo:
                        fieldLength = 10;
                        goto default;
                    case NativeDbType.Logical:
                        fieldLength = 1;
                        goto default;
                    default:
                        dataType = (byte)value;
                        break;
                }
            }
        }

        /**
         Returns field length.
         
         @return field length as int.
         */

        public int FieldLength
        {
            get
            {
                switch (DataType)
                {
                    case NativeDbType.UnicodeChar:
                        return fieldLength + (decimalCount * 256);
                    case NativeDbType.Char:
                        return fieldLength + (decimalCount * 256);
                    default:
                        return fieldLength;
                }
            }
            /**
             Length of the field.
             This method should be called before calling setDecimalCount().
             
             @param Length of the field as int.
             */
            set
            {
                if (value <= 0)
                {
                    throw new ArgumentException(
                        "Field length should be a positive number");
                }

                switch (DataType)
                {
                    case NativeDbType.Date:
                    case NativeDbType.Memo:
                    case NativeDbType.Logical:
                        throw new NotSupportedException(
                            "Cannot set length on this type of field");
                    case NativeDbType.UnicodeChar when value > 255:
                        fieldLength = value % 256;
                        decimalCount = (byte)(value / 256);
                        break;
                    case NativeDbType.Char when value > 255:
                        fieldLength = value % 256;
                        decimalCount = (byte)(value / 256);
                        break;
                    default:
                        fieldLength = value;
                        break;

                }
            }
        }

        /**
         Returns the decimal part. This is applicable
         only if the field type if of numeric in nature.
         
         If the field is specified to hold integral values
         the value returned by this method will be zero.
         
         @return decimal field size as int.
         */

        public int DecimalCount
        {
            get { return decimalCount; }
            /**
             Sets the decimal place size of the field.
             Before calling this method the size of the field
             should be set by calling setFieldLength().
             
             @param Size of the decimal field.
             */
            set
            {
                if (value < 0)
                {
                    throw new ArgumentException(
                        "Decimal length should be a positive number");
                }

                if (value > fieldLength)
                {
                    throw new ArgumentException(
                        "Decimal length should be less than field length");
                }

                decimalCount = (byte)value;
            }
        }

        public bool Read(BinaryReader aReader)
        {
            var t_byte = aReader.ReadByte(); /* 0 */
            if (t_byte == DBFFieldType.EndOfField)
            {
                //System.out.println( "End of header found");
                return false;
            }
            aReader.Read(fieldName, 1, 10); /* 1-10 */
            /* 11 bytes
Field name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00). */

            fieldName[0] = t_byte;

            for (var i = 0; i < fieldName.Length; i++)
            {
                if (fieldName[i]
                    == 0)
                {
                    nameNullIndex = i;
                    break;
                }
            }

            dataType = aReader.ReadByte(); /* 11 */
            /* 1 byte
Field type: 
C   �C   Character
Y   �C   Currency
N   �C   Numeric
F   �C   Float
D   �C   Date
T   �C   DateTime
B   �C   Double
I   �C   Integer
L   �C   Logical
M   �C Memo
G   �C General
C   �C   Character (binary)
M   �C   Memo (binary)
P   �C   Picture
+   �C   Autoincrement (dBase Level 7)
O   �C   Double (dBase Level 7)
@   �C   Timestamp (dBase Level 7) */
            reserv1 = aReader.ReadInt32(); /* 12-15 */
            /* 3 bytes
Displacement of field in record */
            fieldLength = aReader.ReadByte(); /* 16 */
            /* 1 byte
Length of field (in bytes) */
            decimalCount = aReader.ReadByte(); /* 17 */
            /* 1 byte
Number of decimal places */
            reserv2 = aReader.ReadInt16(); /* 18-19 */
            /* 1 byte
Field flags:
0x01   System Column (not visible to user)
0x02   Column can store null values
0x04   Binary column (for CHAR and MEMO only) 
0x06   (0x02+0x04) When a field is NULL and binary (Integer, Currency, and Character/Memo fields)
0x0C   Column is autoincrementing */
            workAreaId = aReader.ReadByte(); /* 20 */
            reserv3 = aReader.ReadInt16(); /* 21-22 */
            /* 4 bytes
Value of autoincrement Next value */
            setFieldsFlag = aReader.ReadByte(); /* 23 */
            /* 1 byte
Value of autoincrement Step value */
            aReader.Read(reserv4, 0, 7); /* 24-30 */
            indexFieldFlag = aReader.ReadByte(); /* 31 */
            /* 8 bytes
Reserved */
            return true;
        }

        /**
         Writes the content of DBFField object into the stream as per
         DBF format specifications.
         
         @param os OutputStream
         @throws IOException if any stream related issues occur.
         */

        public void Write(BinaryWriter aWriter)
        {
            // Field Name
            aWriter.Write(fieldName); /* 0-10 */
            aWriter.Write(new byte[11 - fieldName.Length],
                          0,
                          11 - fieldName.Length);
            /* 11 bytes
Field name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00). */

            // data type
            aWriter.Write(dataType); /* 11 */
            /* 1 byte
Field type: 
C   �C   Character
Y   �C   Currency
N   �C   Numeric
F   �C   Float
D   �C   Date
T   �C   DateTime
B   �C   Double
I   �C   Integer
L   �C   Logical
M   �C Memo
G   �C General
C   �C   Character (binary)
M   �C   Memo (binary)
P   �C   Picture
+   �C   Autoincrement (dBase Level 7)
O   �C   Double (dBase Level 7)
@   �C   Timestamp (dBase Level 7) */
            aWriter.Write(reserv1); /* 12-15 */
            /* 3 bytes
Displacement of field in record */
            aWriter.Write((byte)fieldLength); /* 16 */
            /* 1 byte
Length of field (in bytes) */
            aWriter.Write(decimalCount); /* 17 */
            /* 1 byte
Number of decimal places */
            aWriter.Write(reserv2); /* 18-19 */
            /* 1 byte
Field flags:
0x01   System Column (not visible to user)
0x02   Column can store null values
0x04   Binary column (for CHAR and MEMO only) 
0x06   (0x02+0x04) When a field is NULL and binary (Integer, Currency, and Character/Memo fields)
0x0C   Column is autoincrementing */
            aWriter.Write(workAreaId); /* 20 */
            aWriter.Write(reserv3); /* 21-22 */
            /* 4 bytes
Value of autoincrement Next value */
            aWriter.Write(setFieldsFlag); /* 23 */
            /* 1 byte
Value of autoincrement Step value */
            aWriter.Write(reserv4); /* 24-30*/
            aWriter.Write(indexFieldFlag); /* 31 */
            /* 8 bytes
Reserved */
        }

        /**
         Creates a DBFField object from the data read from the given DataInputStream.
         
         The data in the DataInputStream object is supposed to be organised correctly
         and the stream "pointer" is supposed to be positioned properly.
         
         @param in DataInputStream
         @return Returns the created DBFField object.
         @throws IOException If any stream reading problems occures.
         */

        internal static DBFField CreateField(BinaryReader aReader)
        {
            var field = new DBFField();
            if (field.Read(aReader))
            {
                return field;
            }
            else
            {
                return null;
            }
        }
    }
}