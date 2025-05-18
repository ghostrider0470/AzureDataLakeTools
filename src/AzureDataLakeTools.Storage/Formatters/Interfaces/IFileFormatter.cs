using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace AzureDataLakeTools.Storage.Formatters.Interfaces
{
    /// <summary>
    /// Defines the base contract for file formatters that handle serialization and deserialization of data.
    /// For Parquet-specific operations, use <see cref="IParquetFileFormatter"/>.
    /// </summary>
    public interface IFileFormatter
    {
        /// <summary>
        /// Gets the file extension associated with this formatter (e.g., ".json").
        /// </summary>
        string FileExtension { get; }

        /// <summary>
        /// Gets the MIME content type associated with this formatter.
        /// </summary>
        string ContentType { get; }

        /// <summary>
        /// Serializes an object to a stream.
        /// </summary>
        /// <typeparam name="T">The type of the object to serialize.</typeparam>
        /// <param name="item">The object to serialize.</param>
        /// <returns>A stream containing the serialized data.</returns>
        Task<Stream> SerializeAsync<T>(T item);

        /// <summary>
        /// Serializes a collection of objects to a stream.
        /// </summary>
        /// <typeparam name="T">The type of the objects to serialize.</typeparam>
        /// <param name="items">The collection of objects to serialize.</param>
        /// <returns>A stream containing the serialized data.</returns>
        Task<Stream> SerializeItemsAsync<T>(IEnumerable<T> items);

        /// <summary>
        /// Deserializes a stream to an object of the specified type.
        /// </summary>
        /// <typeparam name="T">The type to deserialize to.</typeparam>
        /// <param name="stream">The stream containing the serialized data.</param>
        /// <returns>The deserialized object.</returns>
        Task<T> DeserializeAsync<T>(Stream stream);

        /// <summary>
        /// Deserializes a stream to a collection of objects of the specified type.
        /// </summary>
        /// <typeparam name="T">The type to deserialize to.</typeparam>
        /// <param name="stream">The stream containing the serialized data.</param>
        /// <returns>A collection of deserialized objects.</returns>
        Task<IEnumerable<T>> DeserializeItemsAsync<T>(Stream stream);
    }
}
