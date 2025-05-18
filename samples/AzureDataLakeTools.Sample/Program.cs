using System.Text.Json;
using Azure.Storage.Files.DataLake;
using AzureDataLakeTools.Storage;
using AzureDataLakeTools.Sample.Models;
using Microsoft.Extensions.Configuration;

// Build configuration
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddUserSecrets<Program>()
    .AddEnvironmentVariables()
    .Build();

// Get configuration values
var connectionString = configuration.GetConnectionString("AzureDataLake") ?? 
                     configuration["AzureDataLake:ConnectionString"];
var fileSystemName = configuration["AzureDataLake:FileSystemName"] ?? "default-container";

if (string.IsNullOrEmpty(connectionString))
{
    Console.Error.WriteLine("Error: Azure Data Lake Storage connection string is not configured.");
    Console.Error.WriteLine("Please set the 'AzureDataLake:ConnectionString' in appsettings.json or user secrets.");
    return 1;
}

try
{
    // Create an instance of AzureDataLakeContext
    var dataLakeContext = new AzureDataLakeContext(configuration);
    
    // Example 1: Store a single item as JSON
    Console.WriteLine("Example 1: Storing a single item as JSON...");
    var sampleData = new SampleData
    {
        Id = 1,
        Name = "Test Item",
        Timestamp = DateTime.UtcNow,
        Value = 123.45,
        IsActive = true,
        Metadata = new Dictionary<string, string>
        {
            { "Category", "Test" },
            { "Priority", "High" }
        }
    };

    var jsonFilePath = await dataLakeContext.StoreItemAsJson(
        sampleData,
        "samples/json",
        fileSystemName,
        "sample-data.json");
    
    Console.WriteLine($"Stored JSON file at: {jsonFilePath}");
    
    // Example 2: Store multiple items as Parquet
    Console.WriteLine("\nExample 2: Storing multiple items as Parquet...");
    var items = Enumerable.Range(1, 10).Select(i => new SampleData
    {
        Id = i,
        Name = $"Item {i}",
        Timestamp = DateTime.UtcNow.AddHours(-i),
        Value = 100 * i,
        IsActive = i % 2 == 0,
        Metadata = new Dictionary<string, string>
        {
            { "Category", $"Category {i % 3}" },
            { "Batch", "20230517" }
        }
    }).ToList();

    var parquetFilePath = await dataLakeContext.StoreItemsAsParquet(
        items,
        "samples/parquet",
        fileSystemName,
        "sample-data.parquet");
    
    Console.WriteLine($"Stored Parquet file at: {parquetFilePath}");
    
    // Example 3: Update the JSON file
    Console.WriteLine("\nExample 3: Updating the JSON file...");
    sampleData.Value = 678.90;
    sampleData.Metadata["Status"] = "Updated";
    
    var updatedJsonPath = await dataLakeContext.UpdateJsonFile(
        sampleData,
        "samples/json/sample-data.json",
        fileSystemName);
    
    Console.WriteLine($"Updated JSON file at: {updatedJsonPath}");
    
    // Example 4: Update the Parquet file
    Console.WriteLine("\nExample 4: Updating the Parquet file...");
    var updatedItems = items.Select(i => 
    {
        i.Value *= 2; // Double the value for demonstration
        return i;
    }).ToList();
    
    var updatedParquetPath = await dataLakeContext.UpdateParquetFile(
        updatedItems,
        "samples/parquet/sample-data.parquet",
        fileSystemName);
    
    Console.WriteLine($"Updated Parquet file at: {updatedParquetPath}");
    
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"An error occurred: {ex.Message}");
    Console.Error.WriteLine(ex.StackTrace);
    return 1;
}
