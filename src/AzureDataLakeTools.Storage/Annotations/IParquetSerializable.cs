using System;
using System.Collections.Generic;
using System.Reflection;
using Parquet.Schema;

namespace AzureDataLakeTools.Storage.Annotations
{
    /// <summary>
    /// Interface that defines how a class should be serialized to Parquet format
    /// </summary>
    /// <typeparam name="TSelf">The type that implements this interface</typeparam>
    public interface IParquetSerializable<TSelf> where TSelf : IParquetSerializable<TSelf>
    {
        /// <summary>
        /// Builds the Parquet schema for this object based on ParquetColumn attributes
        /// </summary>
        /// <returns>The Parquet schema definition</returns>
        public ParquetSchema BuildSchema()
        {
            var type = typeof(TSelf);
            var fields = new List<DataField>();
            
            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                var attr = prop.GetCustomAttribute<ParquetColumnAttribute>();
                if (attr != null)
                {
                    fields.Add(attr.CreateDataField(prop.PropertyType));
                }
            }
            
            if (fields.Count == 0)
            {
                throw new InvalidOperationException($"No properties with {nameof(ParquetColumnAttribute)} found on type {type.Name}");
            }
            
            return new ParquetSchema(fields.ToArray());
        }
    }
}
