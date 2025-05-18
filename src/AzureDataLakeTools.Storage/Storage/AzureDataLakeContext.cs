using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Parquet;
using Parquet.Data;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Parquet.Data;
using Parquet.Schema;
using Array = System.Array;
using AzureDataLakeTools.Storage.Annotations;
using AzureDataLakeTools.Storage.Formatters;
using AzureDataLakeTools.Storage.Formatters.Interfaces;
using AzureDataLakeTools.Storage.Formatters.Json;

namespace AzureDataLakeTools.Storage;

/// <summary>
///     Provides methods to interact with Azure Data Lake Storage.
/// </summary>
/// <summary>
/// Provides methods to interact with Azure Data Lake Storage, supporting both JSON and Parquet file formats.
/// </summary>
public class AzureDataLakeContext : IAzureDataLakeContext, IDisposable
{
    private bool _disposed = false;
    private readonly IConfiguration _configuration;
    private const int DefaultBufferSize = 81920; // 80KB buffer size for uploading
    private readonly ConcurrentDictionary<string, DataLakeFileSystemClient> _fileSystemClients = new();
    private readonly ConcurrentDictionary<string, DataLakeServiceClient> _serviceClients = new();
    private readonly ILogger<AzureDataLakeContext> _logger;
    private readonly IFileFormatter _jsonFormatter;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureDataLakeContext" /> class.
    /// </summary>
    /// <param name="configuration">The configuration containing the Data Lake connection string.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="jsonFormatter">Optional. The JSON formatter to use. If not provided, a default one will be created.</param>
    /// <param name="parquetFormatter">Optional. The Parquet formatter to use. If not provided, a default one will be created.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the Data Lake connection string is not found in the configuration.</exception>
    public AzureDataLakeContext(
        IConfiguration configuration, 
        ILogger<AzureDataLakeContext> logger,
        IFileFormatter? jsonFormatter = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

        // Try to get connection string from configuration
        var connectionString = _configuration["DataLakeConnectionString"] ??
                               _configuration["DataLake:ConnectionString"];

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "Data Lake connection string not found. Please set 'DataLakeConnectionString' or 'DataLake:ConnectionString' in the configuration.");
        }

        // Initialize the default service client
        GetOrCreateServiceClient(connectionString);
        
        // Initialize formatters
        _jsonFormatter = jsonFormatter ?? new JsonFileFormatter();
    }

    /// <summary>
    ///     Gets or creates a DataLakeServiceClient for the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to the storage account.</param>
    /// <returns>A <see cref="DataLakeServiceClient" /> instance.</returns>
    public DataLakeServiceClient GetOrCreateServiceClient(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        return _serviceClients.GetOrAdd(connectionString, cs => new DataLakeServiceClient(cs));
    }

    /// <summary>
    ///     Gets or creates a DataLakeFileSystemClient for the specified file system.
    /// </summary>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="connectionString">
    ///     Optional. The connection string to use. If not provided, uses the default connection
    ///     string.
    /// </param>
    /// <returns>A <see cref="DataLakeFileSystemClient" /> instance.</returns>
    public DataLakeFileSystemClient GetOrCreateFileSystemClient(
        string fileSystemName,
        string? connectionString = null)
    {
        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var cacheKey = $"{connectionString ?? "default"}:{fileSystemName}";

        var fileSystemClient = _fileSystemClients.GetOrAdd(cacheKey, _ =>
        {
            var serviceClient = !string.IsNullOrEmpty(connectionString)
                ? GetOrCreateServiceClient(connectionString)
                : _serviceClients.Values.First();

            return serviceClient.GetFileSystemClient(fileSystemName);
        });

        return fileSystemClient;
    }

    

    /// <inheritdoc />
    public async Task<string> StoreItemAsJson<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        JsonSerializerSettings? jsonSettings = null,
        bool overwrite = true)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }


        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);
        await directoryClient.CreateIfNotExistsAsync();

        fileName ??= $"{Guid.NewGuid()}.json";
        if (!fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            fileName += ".json";
        }

        var fileClient = directoryClient.GetFileClient(fileName);

        // Use the JSON formatter to serialize the item
        using var stream = await _jsonFormatter.SerializeAsync(item);
        await fileClient.UploadAsync(stream, overwrite);

        _logger.LogInformation("Successfully stored JSON file at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);
            
        return fileClient.Path;
    }
    
    /// <inheritdoc />
    public async Task<string> UpdateJsonFile<T>(
        T item,
        string filePath,
        string fileSystemName,
        JsonSerializerSettings? jsonSettings = null)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }


        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Use the JSON formatter to serialize the item
        using var stream = await _jsonFormatter.SerializeAsync(item);
        await fileClient.UploadAsync(stream, overwrite: true);
        
        _logger.LogInformation("Successfully updated JSON file at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return fileClient.Path;
    }
    
    /// <summary>
    /// Disposes the resources used by the AzureDataLakeContext.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
    
    /// <summary>
    /// Disposes the resources used by the AzureDataLakeContext.
    /// </summary>
    /// <param name="disposing">True to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _serviceClients.Clear();
                _fileSystemClients.Clear();
            }
            
            _disposed = true;
        }
    }
    
}
