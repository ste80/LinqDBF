/*
 Serves as the base class of DBFReader adn DBFWriter.
 
 This file is part of DotNetDBF packege.
 
 original author (javadbf): anil@linuxense.com 2004/03/31
 license: LGPL (http://www.gnu.org/copyleft/lesser.html)

 Support for choosing implemented character Sets as
 suggested by Nick Voznesensky <darkers@mail.ru>
 
 ported to C# (DotNetDBF): Jay Tuley <jay+dotnetdbf@tuley.name> 6/28/2007
 
 */
/**
 Base class for DBFReader and DBFWriter.
 */

using System;
using System.Text;

namespace LinqDBF
{
    public abstract class DBFBase
    {
        protected Encoding _CharEncoding = Encoding.ASCII; //Encoding.GetEncoding(1252);
        protected Encoding _UCharEncoding = Encoding.Unicode;
        protected int _BlockSize = 512;
        protected string _NullSymbol;

        public Encoding CharEncoding
        {
            get => _CharEncoding;
            set => _CharEncoding = value;
        }

        public int BlockSize
        {
            get => _BlockSize;
            set => _BlockSize = value;
        }

        public string NullSymbol
        {
            get => _NullSymbol ?? DBFFieldType.Unknown;
            set
            {
                if (value != null && value.Length != 1)
                {
                    throw new ArgumentException(nameof(NullSymbol));
                }
                _NullSymbol = value;
            }
        }

    }
}