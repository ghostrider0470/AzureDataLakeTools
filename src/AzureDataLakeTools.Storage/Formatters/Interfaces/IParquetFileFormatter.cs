using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using AzureDataLakeTools.Storage.Annotations;

namespace AzureDataLakeTools.Storage.Formatters.Interfaces
{
    /// <summary>
    /// Defines the contract for formatters that handle Parquet file serialization and deserialization.
    /// </summary>
    public interface IParquetFileFormatter
    {
        /// <summary>
        /// Gets the file extension for Parquet files.
        /// </summary>
        public string FileExtension => ".parquet";

        /// <summary>
        /// Gets the MIME content type for Parquet files.
        /// </summary>
        public string ContentType => "application/octet-stream";
        
        /// <summary>
        /// Serializes a single item that implements IParquetSerializable to a stream.
        /// </summary>
        /// <typeparam name="T">The type of the item to serialize, which must implement IParquetSerializable<T>.</typeparam>
        /// <param name="item">The item to serialize.</param>
        /// <returns>A stream containing the serialized data.</returns>
        Task<Stream> SerializeAsync<T>(T item) where T : IParquetSerializable<T>;

        /// <summary>
        /// Serializes a collection of items that implement IParquetSerializable to a stream.
        /// </summary>
        /// <typeparam name="T">The type of the items to serialize, which must implement IParquetSerializable<T>.</typeparam>
        /// <param name="items">The collection of items to serialize.</param>
        /// <returns>A stream containing the serialized data.</returns>
        Task<Stream> SerializeItemsAsync<T>(IEnumerable<T> items) where T : IParquetSerializable<T>;

        /// <summary>
        /// Deserializes a stream to a single item that implements IParquetSerializable.
        /// </summary>
        /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable<T> and have a parameterless constructor.</typeparam>
        /// <param name="stream">The stream containing the serialized data.</param>
        /// <returns>The deserialized item.</returns>
        Task<T> DeserializeAsync<T>(Stream stream) where T : IParquetSerializable<T>, new();

        /// <summary>
        /// Deserializes a stream to a collection of items that implement IParquetSerializable.
        /// </summary>
        /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable<T> and have a parameterless constructor.</typeparam>
        /// <param name="stream">The stream containing the serialized data.</param>
        /// <returns>A collection of deserialized items.</returns>
        Task<IEnumerable<T>> DeserializeItemsAsync<T>(Stream stream) where T : IParquetSerializable<T>, new();
    }
}
