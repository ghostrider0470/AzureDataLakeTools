using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AzureDataLakeTools.Storage.Annotations;
using AzureDataLakeTools.Storage.Formatters.Interfaces;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace AzureDataLakeTools.Storage.Formatters.Parquet
{
    /// <summary>
    /// Implementation of IParquetFileFormatter for handling Parquet file operations.
    /// </summary>
    public class ParquetFileFormatter : IParquetFileFormatter
    {
        public async Task<Stream> SerializeAsync<T>(T item) where T : IParquetSerializable<T>
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            var items = new[] { item };
            return await SerializeItemsAsync(items);
        }

        public async Task<Stream> SerializeItemsAsync<T>(IEnumerable<T> items) where T : IParquetSerializable<T>
        {
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }

            var itemList = items.ToList();
            if (!itemList.Any())
            {
                throw new ArgumentException("At least one item is required", nameof(items));
            }

            var schema = GetSchema(itemList.First()!);
            var dataColumns = CreateDataColumns(itemList, schema);

            var stream = new MemoryStream();
            using (var writer = await ParquetWriter.CreateAsync(schema, stream))
            {
                // Create a new row group in the parquet file
                using (ParquetRowGroupWriter groupWriter = writer.CreateRowGroup())
                {
                    foreach (var column in dataColumns)
                    {
                        await groupWriter.WriteColumnAsync(column);
                    }
                }
            }

            stream.Position = 0;
            return stream;
        }

        private static ParquetSchema GetSchema<T>(T item) where T : IParquetSerializable<T>
        {
            if (item is IParquetSerializable<T> serializable)
            {
                return serializable.BuildSchema();
            }

            // Fallback to reflection-based schema generation
            var type = typeof(T);
            var fields = new List<DataField>();
            var properties = type.GetProperties()
                .Where(p => p.CanRead && p.CanWrite && !p.GetIndexParameters().Any());

            foreach (var prop in properties)
            {
                var field = new DataField(
                    prop.Name,
                    GetParquetDataType(prop.PropertyType),
                    IsNullable(prop.PropertyType));
                
                fields.Add(field);
            }

            if (fields.Count == 0)
            {
                throw new InvalidOperationException($"No serializable properties found on type {type.Name}");
            }

            return new ParquetSchema(fields);
        }

        private static List<DataColumn> CreateDataColumns<T>(IEnumerable<T> items, ParquetSchema schema) where T : IParquetSerializable<T>
        {
            var columns = new List<DataColumn>();
            var itemList = items.ToList();
            var properties = typeof(T).GetProperties()
                .Where(p => p.CanRead && p.CanWrite && !p.GetIndexParameters().Any())
                .ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);

            foreach (DataField field in schema.GetDataFields())
            {
                if (!properties.TryGetValue(field.Name, out var property))
                    continue;

                var data = Array.CreateInstance(GetUnderlyingType(property.PropertyType), itemList.Count);
                
                for (int i = 0; i < itemList.Count; i++)
                {
                    var value = property.GetValue(itemList[i]);
                    data.SetValue(value ?? GetDefaultValue(property.PropertyType), i);
                }

                columns.Add(new DataColumn(field, data));
            }

            return columns;
        }

        private static Type GetParquetDataType(Type type)
        {
            // Handle nullable types
            var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            // Map .NET types to Parquet types
            if (underlyingType == typeof(int)) return typeof(int);
            if (underlyingType == typeof(long)) return typeof(long);
            if (underlyingType == typeof(float)) return typeof(float);
            if (underlyingType == typeof(double)) return typeof(double);
            if (underlyingType == typeof(decimal)) return typeof(decimal);
            if (underlyingType == typeof(bool)) return typeof(bool);
            if (underlyingType == typeof(DateTime)) return typeof(DateTime);
            if (underlyingType == typeof(DateTimeOffset)) return typeof(DateTimeOffset);
            if (underlyingType == typeof(Guid)) return typeof(Guid);
            if (underlyingType == typeof(byte[])) return typeof(byte[]);
            
            // Default to string for all other types
            return typeof(string);
        }

        private static bool IsNullable(Type type)
        {
            if (!type.IsValueType) return true; // Reference types are nullable
            return Nullable.GetUnderlyingType(type) != null; // Nullable<T> types
        }

        private static Type GetUnderlyingType(Type type)
        {
            return Nullable.GetUnderlyingType(type) ?? type;
        }

        private static object? GetDefaultValue(Type type)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null;
        }

        public async Task<T> DeserializeAsync<T>(Stream stream) where T : IParquetSerializable<T>, new()
        {
            var items = await DeserializeItemsAsync<T>(stream);
            return items.FirstOrDefault() ?? throw new InvalidOperationException("No items found in the Parquet file");
        }

        public async Task<IEnumerable<T>> DeserializeItemsAsync<T>(Stream stream) where T : IParquetSerializable<T>, new()
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            var result = new List<T>();

            using (var parquetReader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true))
            {
                // Read the first row group
                using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(0))
                {
                    var schema = parquetReader.Schema;
                    var properties = typeof(T).GetProperties();
                    var items = new List<Dictionary<string, object>>();

                    // Read each field from the parquet file
                    foreach (DataField field in schema.GetDataFields())
                    {
                        var property = properties.FirstOrDefault(p => 
                            string.Equals(p.Name, field.Name, StringComparison.OrdinalIgnoreCase));
                        
                        if (property == null) continue;

                        var data = (await groupReader.ReadColumnAsync(field)).Data;
                        
                        for (int i = 0; i < data.Length; i++)
                        {
                            if (i >= items.Count)
                            {
                                items.Add(new Dictionary<string, object>());
                            }
                            
                            items[i][property.Name] = data.GetValue(i) ?? DBNull.Value;
                        }
                    }

                    // Convert the dictionaries to strongly-typed objects
                    foreach (var item in items)
                    {
                        var obj = Activator.CreateInstance<T>();
                        foreach (var prop in properties)
                        {
                            if (item.TryGetValue(prop.Name, out var value) && value != DBNull.Value)
                            {
                                prop.SetValue(obj, Convert.ChangeType(value, prop.PropertyType));
                            }
                        }
                        result.Add(obj);
                    }
                }
            }

            return result;
        }
    }
}