using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace DataAdaptor
{
    public class DataAdaptorDb
    {

        private const string DatabaseName = "TimeSeriesData";
        //private const string EndpointUrl = "https://tsservice.documents.azure.com:443/";
        private const string EndpointUrl = "https://localhost:8081/";

        //private const string PrimaryKey =
        //    "ixUNu9piv6Rh5dEfpqikLV7GHjyNQEAVuWtLFJS1krknQLgNKJqq2n6DeQB3DA7Inleh9U2Xll7e2V47VHY2dw==";
        private const string PrimaryKey =
            "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        private readonly DocumentClient _client;

        public DataAdaptorDb()
        {
            _client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
            GetDbInit().Wait();
        }

        #region Document Queries
        public async void CreateDocument(string collectionLink, object value)
        {
            Document created = await _client.CreateDocumentAsync(collectionLink, value);
        }
        private string GenerateQueryString(string collectionName, string property, string value)
        {
            string query = $"SELECT*FROM{collectionName}f WHERE f.{property}={value}";
            return query;
        }
        private Uri GetDocumentUri(string databaseId, string collectionId, string documentId)
        {
            return UriFactory.CreateDocumentUri(databaseId, collectionId, documentId);
        }
        private async void ReadDocument()
        {
            //var response = await _client.ReadDocumentAsync();
        }

        private async Task<string> ReadDocumentFeed(string collectionLink)
        {
            var result = await _client.ReadDocumentFeedAsync(collectionLink, new FeedOptions { MaxItemCount = 10 });
            List<Document> documents = new List<Document>();
            foreach (Document doc in result)
            {
                Console.WriteLine(doc);
                documents.Add(doc);
            }
            return JsonConvert.SerializeObject(documents);
        }

        public async Task<string> GetAllDocuments(string collectionLink, object start, object end)
        {
            return await ReadDocumentFeed(collectionLink);
        }
        public async Task<string> GetAllDocuments(string collectionLink)
        {
            return await ReadDocumentFeed(collectionLink);
        }

        public async Task<string> GetCollectionLink(string tagName)
        {
            return await GetDbCollectionInit(tagName);
        }
        #endregion

        #region Collection Queries
        private async Task<string> GetDbCollectionInit(string collectionName)
        {
            var result =
                await _client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(DatabaseName),
                    new DocumentCollection { Id = collectionName });
            return result.Resource.SelfLink;
        }
        public async Task<List<string>> GetAllCollections()
        {
            var colls = await _client.ReadDocumentCollectionFeedAsync(UriFactory.CreateDatabaseUri(DatabaseName));
            Console.WriteLine("\n5. Reading all DocumentCollection resources for a database");
            foreach (var coll in colls)
            {
                Console.WriteLine(coll.Id);
            }
            return colls.Select(x=>x.Id).ToList();
        }
        public async void DeleteCollection(DocumentCollection collection)
        {
            await _client.DeleteDocumentCollectionAsync(collection.SelfLink);
        }
        public async Task<string> CreateDocumentCollection(string name)
        {
            var selflink = await GetDbCollectionInit(name);
            return selflink;
        }
        #endregion

        #region Database Queries
        private async Task GetDbInit()
        {
            await _client.CreateDatabaseIfNotExistsAsync(new Database { Id = DatabaseName });
        }
        #endregion
    }
}
