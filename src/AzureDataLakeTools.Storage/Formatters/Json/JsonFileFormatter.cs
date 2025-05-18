using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureDataLakeTools.Storage.Formatters.Interfaces;
using Newtonsoft.Json;

namespace AzureDataLakeTools.Storage.Formatters.Json
{
    /// <summary>
    /// JSON implementation of the IFileFormatter interface.
    /// </summary>
    public class JsonFileFormatter : IFileFormatter
    {
        private readonly JsonSerializerSettings _jsonSettings;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonFileFormatter"/> class.
        /// </summary>
        /// <param name="jsonSettings">Optional JSON serializer settings.</param>
        public JsonFileFormatter(JsonSerializerSettings jsonSettings = null)
        {
            _jsonSettings = jsonSettings ?? new JsonSerializerSettings
            {
                Formatting = Formatting.Indented,
                NullValueHandling = NullValueHandling.Ignore
            };
        }

        /// <inheritdoc />
        public string FileExtension => ".json";

        /// <inheritdoc />
        public string ContentType => "application/json";

        /// <inheritdoc />
        public async Task<Stream> SerializeAsync<T>(T item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            return await SerializeItemsAsync(new[] { item });
        }

        /// <inheritdoc />
        public async Task<Stream> SerializeItemsAsync<T>(IEnumerable<T> items)
        {
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }


            var json = JsonConvert.SerializeObject(items, _jsonSettings);
            var stream = new MemoryStream();
            await using var writer = new StreamWriter(stream, Encoding.UTF8, 8192, leaveOpen: true);
            await writer.WriteAsync(json);
            await writer.FlushAsync();
            stream.Position = 0;
            return stream;
        }

        /// <inheritdoc />
        public async Task<T> DeserializeAsync<T>(Stream stream)
        {
            var items = await DeserializeItemsAsync<T>(stream);
            return items.FirstOrDefault()!;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<T>> DeserializeItemsAsync<T>(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }


            using var reader = new StreamReader(stream, Encoding.UTF8, true, 8192, leaveOpen: true);
            var json = await reader.ReadToEndAsync();
            
            // Check if we're deserializing to a collection type
            var type = typeof(T);
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>) ||
                type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ICollection<>) ||
                type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IList<>) ||
                type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
            {
                return JsonConvert.DeserializeObject<IEnumerable<T>>(json, _jsonSettings) ?? Array.Empty<T>();
            }
            
            // For single items, wrap in an array
            var item = JsonConvert.DeserializeObject<T>(json, _jsonSettings);
            return item != null ? new[] { item } : Array.Empty<T>();
        }
    }
}
