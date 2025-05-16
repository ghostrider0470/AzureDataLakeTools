# Azure Data Lake Tools - Sample Application

This sample application demonstrates how to use the `AzureDataLakeTools.Storage` library to interact with Azure Data Lake Storage.

## Prerequisites

- .NET 6.0 SDK or later
- An Azure Storage account with Data Lake Storage Gen2 enabled
- The connection string or account key for your Azure Storage account

## Configuration

1. Update the `appsettings.json` file with your Azure Storage account details:

```json
{
  "AzureDataLake": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=yourstorageaccount;AccountKey=yourstoragekey;EndpointSuffix=core.windows.net",
    "FileSystemName": "yourfilesystem"
  }
}
```

Alternatively, you can use user secrets for sensitive information:

```bash
dotnet user-secrets init
dotnet user-secrets set "AzureDataLake:ConnectionString" "your_connection_string_here"
```

## Running the Sample

1. Build the solution:
   ```bash
   dotnet build
   ```

2. Run the sample application:
   ```bash
   dotnet run --project samples/AzureDataLakeTools.Sample
   ```

## What the Sample Does

The sample demonstrates the following operations:

1. **Store a single item as JSON**
   - Creates a sample object and stores it as a JSON file in Azure Data Lake Storage

2. **Store multiple items as Parquet**
   - Creates a collection of sample objects and stores them as a Parquet file

3. **Check if files exist**
   - Verifies that the created files exist in the storage account

4. **List files in a directory**
   - Recursively lists all files in the samples directory

## Example Output

```
Example 1: Storing a single item as JSON...
Stored JSON file at: samples/json/sample-data.json

Example 2: Storing multiple items as Parquet...
Stored Parquet file at: samples/parquet/sample-data.parquet

Example 3: Checking if files exist...
JSON file exists: True
Parquet file exists: True

Example 4: Listing files in directory...
Files in 'samples' directory:
- samples/json/sample-data.json
- samples/parquet/sample-data.parquet
```

## Next Steps

- Explore more operations in the `AzureDataLakeContext` class
- Add error handling and retry policies for production use
- Implement logging for better observability
- Add unit tests for your implementation
