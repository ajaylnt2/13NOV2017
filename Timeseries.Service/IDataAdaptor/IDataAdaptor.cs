namespace IDataAdaptor
{
    public interface IDataAdaptor
    {
        string CreateDocumentCollection(string collectionName);
        void CreateDocument(string collectionLink,object jsonvalue);
    }
}
