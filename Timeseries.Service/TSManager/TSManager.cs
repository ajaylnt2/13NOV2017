using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DataAdaptor;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TSManager
{
    public class TsManager
    {
        private readonly DataAdaptorDb _dataAdaptor;

        public TsManager()
        {
            _dataAdaptor = new DataAdaptorDb();
        }

        public async Task<List<string>> ParseJson(object jsonvalue)
        {
            List<string> assetsCreatedList = new List<string>();
            var serialized = JsonConvert.SerializeObject(jsonvalue);
            JToken entireJson = JToken.Parse(serialized);

            var collectionLink = string.Empty;

            foreach (var item in entireJson)
            {
                if (item is JProperty)
                {
                    var key = (item as JProperty).Name;
                    var value = (item as JProperty).Value.ToString();
                    if (key == "TagName")
                    {
                        string[] tokens = value.Split('/').Where(x => !string.IsNullOrEmpty(x)).ToArray();
                        var baseCollectionName = tokens[0];
                        collectionLink = await _dataAdaptor.CreateDocumentCollection(baseCollectionName);
                        _dataAdaptor.CreateDocument(collectionLink, jsonvalue);
                    }
                }
                else
                {
                    var x = item.Value<JObject>();
                    foreach (var y in x)
                    {
                        var key = y.Key;
                        var value = y.Value;
                        if (key == "TagName")
                        {
                            string[] tokens = value.ToString().Split('/').Where(x1 => !string.IsNullOrEmpty(x1))
                                .ToArray();
                            var baseCollectionName = tokens[0];
                            collectionLink = await _dataAdaptor.CreateDocumentCollection(baseCollectionName);
                            _dataAdaptor.CreateDocument(collectionLink, item);
                        }
                    }
                }
            }
            return assetsCreatedList;
        }

        public async Task<List<string>> GetAllTags()
        {
            return await _dataAdaptor.GetAllCollections();
        }

        public async Task<IList<string>> GetDataPoints(TsQueryModel query)
        {
            var tag = query.Tags;
            return await GetDocuments(tag.Name);
        }
        private async Task<IList<string>> GetDocuments(string[] collectionName)
        {
            IList<string> resultList = new List<string>();
            foreach (var name in collectionName)
            {
                var collectionLink = await _dataAdaptor.GetCollectionLink(name);
                resultList.Add(await _dataAdaptor.GetAllDocuments(collectionLink));
            }
            return resultList;

        }
    }
}
