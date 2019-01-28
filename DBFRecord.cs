using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LinqDBF
{
    public class DBFRecord
    {
        private Dictionary<string, int> LookupFieldName { get; set; }
        public object[] ValueArray { get; set; }
        public long Position { get; set; }

        private readonly List<int> Dirty = new List<int>();
        private bool Deleted = false;

        public DBFRecord(Dictionary<string, int> fieldNameLookup, object[] objects, long position)
        {
            LookupFieldName = fieldNameLookup;
            ValueArray = objects;
            Position = position;
        }

        public bool IsDelete()
        {
            return Deleted;
        }

        public void Delete()
        {
            Deleted = true;
        }

        public void UnDelete()
        {
            Deleted = false;
        }

        public bool IsDirty()
        {
            return Dirty.Count > 0;
        }

        public bool IsDirty(int fieldIndex)
        {
            return Dirty.Contains(fieldIndex);
        }

        public object Get(string fieldName)
        {
            return Get(fieldName, null);
        }

        public object Get(string fieldName, object defaultValue)
        {
            return ValueArray == null ? defaultValue :
                LookupFieldName.TryGetValue(fieldName, out int idx) ?
                ValueArray[idx] : defaultValue;
        }

        public T Get<T>(string fieldName)
        {
            return Get<T>(fieldName, default(T));
        }

        public T Get<T>(string fieldName, T defaultValue)
        {
            return ValueArray == null ? defaultValue :
                LookupFieldName.TryGetValue(fieldName, out int idx) ?
                (T)ValueArray[idx] : defaultValue;
        }


        public object Get(int fieldIndex)
        {
            return Get(fieldIndex, null);
        }

        public object Get(int fieldIndex, object defaultValue)
        {
            return ValueArray == null ? defaultValue : ValueArray[fieldIndex];
        }

        public T Get<T>(int fieldIndex)
        {
            return Get<T>(fieldIndex, default(T));
        }

        public T Get<T>(int fieldIndex, T defaultValue)
        {
            return (T)Get(fieldIndex, defaultValue);
        }
        

        public void Set(string fieldName, object newValue)
        {
            if (LookupFieldName.TryGetValue(fieldName, out int idx))
            {
                Set(idx, newValue);
            }
        }

        public void Set(int fieldIndex, object newValue)
        {
            ValueArray[fieldIndex] = newValue;
            if (Dirty.Contains(fieldIndex) != true)
            {
                Dirty.Add(fieldIndex);
            }
        }

        public JObject AsJObject()
        {
            var objRecord = new JObject();
            foreach (var item in LookupFieldName)
            {
                objRecord[item.Key] = ValueArray[item.Value] == null ? null :
                    JToken.FromObject(ValueArray[item.Value]);
            }
            return objRecord;
        }

        public override string ToString()
        {
            return AsJObject().ToString(0);
        }
    }
}
