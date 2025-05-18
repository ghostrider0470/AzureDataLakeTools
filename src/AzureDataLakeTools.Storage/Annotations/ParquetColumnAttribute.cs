using System;
using Parquet.Data;
using Parquet.Schema;

namespace AzureDataLakeTools.Storage.Annotations
{
    /// <summary>
    /// Specifies that a property should be included in the Parquet schema
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ParquetColumnAttribute : Attribute
    {
        /// <summary>
        /// The name of the column in the Parquet file
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Whether the column is nullable
        /// </summary>
        public bool Nullable { get; set; } = true;

        /// <summary>
        /// The data type of the column. If not specified, it will be inferred from the property type.
        /// </summary>
        public Type DataType { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetColumnAttribute"/> class.
        /// </summary>
        /// <param name="name">The name of the column in the Parquet file</param>
        public ParquetColumnAttribute(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        /// <summary>
        /// Creates a DataField based on the attribute's configuration
        /// </summary>
        /// <param name="propertyType">The type of the property</param>
        /// <returns>A DataField instance</returns>
        public DataField CreateDataField(Type propertyType = null)
        {
            var type = DataType ?? propertyType ?? throw new ArgumentNullException(nameof(propertyType));

            // Map .NET types to Parquet types
            if (type == typeof(int) || type == typeof(int?))
                return new DataField<int>(Name, Nullable);
            if (type == typeof(long) || type == typeof(long?))
                return new DataField<long>(Name, Nullable);
            if (type == typeof(string))
                return new DataField<string>(Name, Nullable);
            if (type == typeof(bool) || type == typeof(bool?))
                return new DataField<bool>(Name, Nullable);
            if (type == typeof(float) || type == typeof(float?))
                return new DataField<float>(Name, Nullable);
            if (type == typeof(double) || type == typeof(double?))
                return new DataField<double>(Name, Nullable);
            if (type == typeof(decimal) || type == typeof(decimal?))
                return new DataField<decimal>(Name, Nullable);
            if (type == typeof(DateTime) || type == typeof(DateTime?))
                return new DataField<DateTime>(Name, Nullable);
            if (type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?))
                return new DataField<DateTimeOffset>(Name, Nullable);
            if (type == typeof(Guid) || type == typeof(Guid?))
                return new DataField<Guid>(Name, Nullable);

            throw new NotSupportedException($"Type {type.Name} is not supported for Parquet serialization");
        }
    }
}
