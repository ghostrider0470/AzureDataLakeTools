namespace AzureDataLakeTools.Sample.Models;

public class SampleData
{
    public int Id { get; set; }
    public string? Name { get; set; }
    public DateTime Timestamp { get; set; }
    public double Value { get; set; }
    public bool IsActive { get; set; }
    public Dictionary<string, string>? Metadata { get; set; }
}
