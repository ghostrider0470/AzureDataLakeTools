using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using AzureDataLakeTools.Storage.Annotations;

namespace AzureDataLakeTools.Storage;

/// <summary>
/// Defines the interface for interacting with Azure Data Lake Storage.
/// </summary>
public interface IAzureDataLakeContext : IDisposable
{
    /// <summary>
    /// Gets or creates a DataLakeServiceClient for the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to the storage account.</param>
    /// <returns>A <see cref="DataLakeServiceClient" /> instance.</returns>
    DataLakeServiceClient GetOrCreateServiceClient(string connectionString);

    /// <summary>
    /// Gets or creates a DataLakeFileSystemClient for the specified file system.
    /// </summary>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="connectionString">
    /// Optional. The connection string to use. If not provided, uses the default connection
    /// string.
    /// </param>
    /// <returns>A <see cref="DataLakeFileSystemClient" /> instance.</returns>
    DataLakeFileSystemClient GetOrCreateFileSystemClient(string fileSystemName, string? connectionString = null);

    /// <summary>
    /// Stores an item as a JSON file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the item to store.</typeparam>
    /// <param name="item">The item to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="jsonSettings">Optional. The JSON serializer settings.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
    Task<string> StoreItemAsJson<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        JsonSerializerSettings? jsonSettings = null,
        bool overwrite = true);

    /// <summary>
    /// Updates an existing JSON file in Azure Data Lake Storage with new content.
    /// </summary>
    /// <typeparam name="T">The type of the item to update with.</typeparam>
    /// <param name="item">The item containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="jsonSettings">Optional. The JSON serializer settings.</param>
    /// <returns>The path to the updated file.</returns>
    Task<string> UpdateJsonFile<T>(
        T item,
        string filePath,
        string fileSystemName,
        JsonSerializerSettings? jsonSettings = null);
        
    /// <summary>
    /// Stores an item as a Parquet file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the item to store, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="item">The item to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
    Task<string> StoreItemAsParquet<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        bool overwrite = true) where T : IParquetSerializable<T>;
    
    /// <summary>
    /// Stores a collection of items as a Parquet file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the items to store, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="items">The collection of items to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
    Task<string> StoreItemsAsParquet<T>(
        IEnumerable<T> items,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        bool overwrite = true) where T : IParquetSerializable<T>;
    
    /// <summary>
    /// Updates an existing Parquet file in Azure Data Lake Storage with new content.
    /// </summary>
    /// <typeparam name="T">The type of the item to update with, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="item">The item containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The path to the updated file.</returns>
    Task<string> UpdateParquetFile<T>(
        T item,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>;
    
    /// <summary>
    /// Updates an existing Parquet file in Azure Data Lake Storage with a collection of items.
    /// </summary>
    /// <typeparam name="T">The type of the items to update with, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="items">The collection of items containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The path to the updated file.</returns>
    Task<string> UpdateParquetFileWithItems<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>;
    
    /// <summary>
    /// Reads a Parquet file from Azure Data Lake Storage and deserializes it to an object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The deserialized object.</returns>
    Task<T> ReadParquetFile<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new();
    
    /// <summary>
    /// Reads a Parquet file from Azure Data Lake Storage and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>A collection of deserialized objects.</returns>
    Task<IEnumerable<T>> ReadParquetItems<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new();
}
