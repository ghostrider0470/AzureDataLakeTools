using System.Collections.Concurrent;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Array = System.Array;

namespace AzureDataLakeTools.Storage;

/// <summary>
///     Provides methods to interact with Azure Data Lake Storage.
/// </summary>
public class AzureDataLakeContext
{
    private readonly IConfiguration _configuration;
    private readonly ConcurrentDictionary<string, DataLakeFileSystemClient> _fileSystemClients = new();
    private readonly ConcurrentDictionary<string, DataLakeServiceClient> _serviceClients = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureDataLakeContext" /> class.
    /// </summary>
    /// <param name="configuration">The configuration containing the Data Lake connection string.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when the Data Lake connection string is not found in the
    ///     configuration.
    /// </exception>
    public AzureDataLakeContext(IConfiguration configuration)
    {
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
    public Task<DataLakeFileSystemClient> GetOrCreateFileSystemClientAsync(string fileSystemName,
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

        return Task.FromResult(fileSystemClient);
    }

    /// <summary>
    ///     Stores an item as a JSON file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the item to store.</typeparam>
    /// <param name="item">The item to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="jsonSettings">Optional. The JSON serializer settings.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
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

        var fileSystemClient = await GetOrCreateFileSystemClientAsync(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);
        await directoryClient.CreateIfNotExistsAsync();

        fileName ??= $"{Guid.NewGuid()}.json";
        if (!fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            fileName += ".json";
        }

        var fileClient = directoryClient.GetFileClient(fileName);

        var json = JsonConvert.SerializeObject(item,
            jsonSettings ?? new JsonSerializerSettings
            {
                Formatting = Formatting.Indented, NullValueHandling = NullValueHandling.Ignore
            });

        using var stream = new MemoryStream();
        using (var writer = new StreamWriter(stream, leaveOpen: true))
        {
            await writer.WriteAsync(json);
            await writer.FlushAsync();
            stream.Position = 0;

            await fileClient.UploadAsync(stream, overwrite);
        }

        return fileClient.Path;
    }

    /// <summary>
    ///     Stores a collection of items as a Parquet file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the items to store.</typeparam>
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
        bool overwrite = true) where T : class
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = await GetOrCreateFileSystemClientAsync(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);
        await directoryClient.CreateIfNotExistsAsync();

        fileName ??= $"{Guid.NewGuid()}.parquet";
        if (!fileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            fileName += ".parquet";
        }

        var fileClient = directoryClient.GetFileClient(fileName);

        using var stream = new MemoryStream();
        var schema = CreateSchema<T>();
        var columns = CreateColumns(items, schema);

        using (var parquetWriter = await ParquetWriter.CreateAsync(schema, stream))
        {
            using var groupWriter = parquetWriter.CreateRowGroup();

            foreach (var column in columns)
            {
                await groupWriter.WriteColumnAsync(column);
            }
        }

        stream.Position = 0;
        await fileClient.UploadAsync(stream, overwrite);

        return fileClient.Path;
    }

    private static ParquetSchema CreateSchema<T>()
    {
        var type = typeof(T);
        var properties = type.GetProperties();
        var fields = new List<DataField>();

        foreach (var prop in properties)
        {
            var dataType = GetParquetDataType(prop.PropertyType);
            fields.Add(new DataField(prop.Name, dataType,
                prop.PropertyType.IsClass && prop.PropertyType != typeof(string)));
        }

        return new ParquetSchema(fields.ToArray());
    }

    private static Type GetParquetDataType(Type type)
    {
        if (type == typeof(int) || type == typeof(int?))
        {
            return typeof(int);
        }

        if (type == typeof(long) || type == typeof(long?))
        {
            return typeof(long);
        }

        if (type == typeof(float) || type == typeof(float?))
        {
            return typeof(float);
        }

        if (type == typeof(double) || type == typeof(double?))
        {
            return typeof(double);
        }

        if (type == typeof(decimal) || type == typeof(decimal?))
        {
            return typeof(decimal);
        }

        if (type == typeof(bool) || type == typeof(bool?))
        {
            return typeof(bool);
        }

        if (type == typeof(DateTime) || type == typeof(DateTime?))
        {
            return typeof(DateTime);
        }

        if (type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?))
        {
            return typeof(DateTimeOffset);
        }

        if (type == typeof(Guid) || type == typeof(Guid?))
        {
            return typeof(Guid);
        }

        if (type == typeof(string))
        {
            return typeof(string);
        }

        return typeof(string); // Default to string for complex types
    }

    private static List<DataColumn> CreateColumns<T>(IEnumerable<T> items, ParquetSchema schema) where T : class
    {
        var columns = new List<DataColumn>();
        var properties = typeof(T).GetProperties();
        var data = items.ToList();

        foreach (var field in schema.GetDataFields())
        {
            var property = properties.FirstOrDefault(p => p.Name == field.Name);
            if (property == null)
            {
                continue;
            }

            var dataType = GetParquetDataType(property.PropertyType);
            var isNullable = Nullable.GetUnderlyingType(property.PropertyType) != null ||
                             (!property.PropertyType.IsValueType && property.PropertyType != typeof(string));

            var array = Array.CreateInstance(property.PropertyType, data.Count);

            for (var i = 0; i < data.Count; i++)
            {
                var value = property.GetValue(data[i]);
                array.SetValue(value ?? (isNullable ? null : GetDefaultValue(property.PropertyType)), i);
            }

            columns.Add(new DataColumn(field, array));
        }

        return columns;
    }

    private static object? GetDefaultValue(Type type)
    {
        if (type.IsValueType)
        {
            return Activator.CreateInstance(type);
        }

        return null;
    }
}
