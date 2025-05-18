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
using AzureDataLakeTools.Storage.Formatters.Parquet;

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
    private readonly IParquetFileFormatter _parquetFormatter;

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
        IFileFormatter? jsonFormatter = null,
        IParquetFileFormatter? parquetFormatter = null)
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
        _parquetFormatter = parquetFormatter ?? new ParquetFileFormatter();
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
    public async Task<string> StoreItemAsParquet<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        bool overwrite = true) where T : IParquetSerializable<T>
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

        fileName ??= $"{Guid.NewGuid()}.parquet";
        if (!fileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            fileName += ".parquet";
        }

        var fileClient = directoryClient.GetFileClient(fileName);

        // Use the Parquet formatter to serialize the item
        using var stream = await _parquetFormatter.SerializeAsync(item);
        
        // Get the length of the stream for proper upload
        var contentLength = stream.Length;
        stream.Position = 0;
        
        try
        {
            if (overwrite)
            {
                // Try to delete if exists, but don't fail if it doesn't
                try
                {
                    if (await fileClient.ExistsAsync())
                    {
                        await fileClient.DeleteAsync();
                    }
                }
                catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
                {
                    // File doesn't exist, which is fine for our purposes
                    _logger.LogDebug("File {Path} didn't exist when trying to delete it, continuing with upload", 
                        fileClient.Path);
                }
            }
            
            // Upload the entire stream at once
            await fileClient.UploadAsync(stream, overwrite: false);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists" && overwrite)
        {
            // If the path already exists and we want to overwrite, try the delete-then-upload approach again
            _logger.LogDebug("Path {Path} already exists, retrying with explicit delete-then-create approach", 
                fileClient.Path);
                
            try
            {
                // Reset stream position
                stream.Position = 0;
                
                // Delete the file explicitly
                await fileClient.DeleteAsync();
                
                // Wait a short time to ensure deletion is processed
                await Task.Delay(100);
                
                // Upload again
                await fileClient.UploadAsync(stream, overwrite: false);
            }
            catch (Exception retryEx)
            {
                throw new InvalidOperationException($"Failed to upload Parquet file after retry: {retryEx.Message}", retryEx);
            }
        }

        _logger.LogInformation("Successfully stored Parquet file at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);
            
        return fileClient.Path;
    }
    
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
    public async Task<string> StoreItemsAsParquet<T>(
        IEnumerable<T> items,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        bool overwrite = true) where T : IParquetSerializable<T>
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        if (!items.Any())
        {
            throw new ArgumentException("At least one item is required", nameof(items));
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

        fileName ??= $"{Guid.NewGuid()}.parquet";
        if (!fileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            fileName += ".parquet";
        }

        var fileClient = directoryClient.GetFileClient(fileName);

        // Use the Parquet formatter to serialize the items
        using var stream = await _parquetFormatter.SerializeItemsAsync(items);
        
        // Get the length of the stream for proper upload
        var contentLength = stream.Length;
        stream.Position = 0;
        
        try
        {
            if (overwrite)
            {
                // Try to delete if exists, but don't fail if it doesn't
                try
                {
                    if (await fileClient.ExistsAsync())
                    {
                        await fileClient.DeleteAsync();
                    }
                }
                catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
                {
                    // File doesn't exist, which is fine for our purposes
                    _logger.LogDebug("File {Path} didn't exist when trying to delete it, continuing with upload", 
                        fileClient.Path);
                }
            }
            
            // Upload the entire stream at once
            await fileClient.UploadAsync(stream, overwrite: false);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists" && overwrite)
        {
            // If the path already exists and we want to overwrite, try the delete-then-upload approach again
            _logger.LogDebug("Path {Path} already exists, retrying with explicit delete-then-create approach", 
                fileClient.Path);
                
            try
            {
                // Reset stream position
                stream.Position = 0;
                
                // Delete the file explicitly
                await fileClient.DeleteAsync();
                
                // Wait a short time to ensure deletion is processed
                await Task.Delay(100);
                
                // Upload again
                await fileClient.UploadAsync(stream, overwrite: false);
            }
            catch (Exception retryEx)
            {
                throw new InvalidOperationException($"Failed to upload Parquet file after retry: {retryEx.Message}", retryEx);
            }
        }

        _logger.LogInformation("Successfully stored Parquet file with multiple items at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);
            
        return fileClient.Path;
    }
    
    /// <summary>
    /// Updates an existing Parquet file in Azure Data Lake Storage with new content.
    /// </summary>
    /// <typeparam name="T">The type of the item to update with, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="item">The item containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The path to the updated file.</returns>
    public async Task<string> UpdateParquetFile<T>(
        T item,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>
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

        // Use the Parquet formatter to serialize the item
        using var stream = await _parquetFormatter.SerializeAsync(item);
        
        // Get the length of the stream for proper upload
        var contentLength = stream.Length;
        stream.Position = 0;
        
        try
        {
            // Try to delete if exists, but don't fail if it doesn't
            try
            {
                if (await fileClient.ExistsAsync())
                {
                    await fileClient.DeleteAsync();
                }
            }
            catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
            {
                // File doesn't exist, which is fine for our purposes
                _logger.LogDebug("File {Path} didn't exist when trying to delete it, continuing with upload", 
                    fileClient.Path);
            }
            
            // Upload the entire stream at once
            await fileClient.UploadAsync(stream, overwrite: false);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists")
        {
            // If the path already exists, try the delete-then-upload approach again
            _logger.LogDebug("Path {Path} already exists, retrying with explicit delete-then-create approach", 
                fileClient.Path);
                
            try
            {
                // Reset stream position
                stream.Position = 0;
                
                // Delete the file explicitly
                await fileClient.DeleteAsync();
                
                // Wait a short time to ensure deletion is processed
                await Task.Delay(100);
                
                // Upload again
                await fileClient.UploadAsync(stream, overwrite: false);
            }
            catch (Exception retryEx)
            {
                throw new InvalidOperationException($"Failed to upload Parquet file after retry: {retryEx.Message}", retryEx);
            }
        }
        
        _logger.LogInformation("Successfully updated Parquet file at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return fileClient.Path;
    }
    
    /// <summary>
    /// Updates an existing Parquet file in Azure Data Lake Storage with a collection of items.
    /// </summary>
    /// <typeparam name="T">The type of the items to update with, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="items">The collection of items containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The path to the updated file.</returns>
    public async Task<string> UpdateParquetFileWithItems<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        if (!items.Any())
        {
            throw new ArgumentException("At least one item is required", nameof(items));
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

        // Use the Parquet formatter to serialize the items
        using var stream = await _parquetFormatter.SerializeItemsAsync(items);
        
        // Get the length of the stream for proper upload
        var contentLength = stream.Length;
        stream.Position = 0;
        
        try
        {
            // Try to delete if exists, but don't fail if it doesn't
            try
            {
                if (await fileClient.ExistsAsync())
                {
                    await fileClient.DeleteAsync();
                }
            }
            catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
            {
                // File doesn't exist, which is fine for our purposes
                _logger.LogDebug("File {Path} didn't exist when trying to delete it, continuing with upload", 
                    fileClient.Path);
            }
            
            // Upload the entire stream at once
            await fileClient.UploadAsync(stream, overwrite: false);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists")
        {
            // If the path already exists, try the delete-then-upload approach again
            _logger.LogDebug("Path {Path} already exists, retrying with explicit delete-then-create approach", 
                fileClient.Path);
                
            try
            {
                // Reset stream position
                stream.Position = 0;
                
                // Delete the file explicitly
                await fileClient.DeleteAsync();
                
                // Wait a short time to ensure deletion is processed
                await Task.Delay(100);
                
                // Upload again
                await fileClient.UploadAsync(stream, overwrite: false);
            }
            catch (Exception retryEx)
            {
                throw new InvalidOperationException($"Failed to upload Parquet file after retry: {retryEx.Message}", retryEx);
            }
        }
        
        _logger.LogInformation("Successfully updated Parquet file with multiple items at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return fileClient.Path;
    }
    
    /// <summary>
    /// Reads a Parquet file from Azure Data Lake Storage and deserializes it to an object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The deserialized object.</returns>
    public async Task<T> ReadParquetFile<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new()
    {
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

        // Download the file to a memory stream
        var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        memoryStream.Position = 0;

        // Use the Parquet formatter to deserialize the stream
        var result = await _parquetFormatter.DeserializeAsync<T>(memoryStream);
        
        _logger.LogInformation("Successfully read Parquet file from {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return result;
    }
    
    /// <summary>
    /// Reads a Parquet file from Azure Data Lake Storage and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>A collection of deserialized objects.</returns>
    public async Task<IEnumerable<T>> ReadParquetItems<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new()
    {
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

        // Download the file to a memory stream
        var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        memoryStream.Position = 0;

        // Use the Parquet formatter to deserialize the stream
        var result = await _parquetFormatter.DeserializeItemsAsync<T>(memoryStream);
        
        _logger.LogInformation("Successfully read Parquet file with multiple items from {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return result;
    }
}
