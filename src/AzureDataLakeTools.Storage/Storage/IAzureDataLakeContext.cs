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
}
