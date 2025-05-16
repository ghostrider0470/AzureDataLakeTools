# Azure Data Lake Tools

A .NET library for working with Azure Data Lake Storage, supporting both JSON and Parquet file formats.

## Features

- Store objects as JSON files in Azure Data Lake Storage
- Store collections of objects as Parquet files
- Thread-safe client caching for better performance
- Simple and intuitive API
- Support for .NET 6.0 and later

## Installation

```bash
dotnet add package AzureDataLakeTools.Storage
```

## Usage

### Configuration

Add your Data Lake connection string to your `appsettings.json`:

```json
{
  "DataLakeConnectionString": "YourConnectionStringHere"
  
  // OR
  
  "DataLake": {
    "ConnectionString": "YourConnectionStringHere"
  }
}
```

### Basic Example

```csharp
using AzureDataLakeTools.Storage;
using Microsoft.Extensions.Configuration;

// Set up configuration
var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .AddEnvironmentVariables()
    .Build();

// Create a new instance of AzureDataLakeContext
var dataLakeContext = new AzureDataLakeContext(configuration);

// Store an object as JSON
var user = new { Id = 1, Name = "John Doe", Email = "john@example.com" };
var jsonPath = await dataLakeContext.StoreItemAsJson(
    user, 
    "users", 
    "myfilesystem", 
    "user_1.json");

Console.WriteLine($"Stored JSON at: {jsonPath}");

// Store a collection as Parquet
var users = new[]
{
    new { Id = 1, Name = "John Doe", Email = "john@example.com" },
    new { Id = 2, Name = "Jane Smith", Email = "jane@example.com" }
};

var parquetPath = await dataLakeContext.StoreItemsAsParquet(
    users, 
    "users", 
    "myfilesystem", 
    "users.parquet");

Console.WriteLine($"Stored Parquet at: {parquetPath}");
```

## API Reference

### AzureDataLakeContext

#### Constructors

- `AzureDataLakeContext(IConfiguration configuration)`
  - `configuration`: The configuration containing the Data Lake connection string.

#### Methods

- `DataLakeServiceClient GetOrCreateServiceClient(string connectionString)`
  - Gets or creates a `DataLakeServiceClient` for the specified connection string.

- `Task<DataLakeFileSystemClient> GetOrCreateFileSystemClientAsync(string fileSystemName, string? connectionString = null)`
  - Gets or creates a `DataLakeFileSystemClient` for the specified file system.

- `Task<string> StoreItemAsJson<T>(T item, string directoryPath, string fileSystemName, string? fileName = null, JsonSerializerSettings? jsonSettings = null, bool overwrite = true)`
  - Stores an item as a JSON file in Azure Data Lake Storage.

- `Task<string> StoreItemsAsParquet<T>(IEnumerable<T> items, string directoryPath, string fileSystemName, string? fileName = null, bool overwrite = true) where T : class`
  - Stores a collection of items as a Parquet file in Azure Data Lake Storage.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
