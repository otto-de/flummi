package de.otto.flummi;


import de.otto.flummi.request.CreateIndexRequestBuilder;
import de.otto.flummi.request.DeleteIndexRequestBuilder;
import de.otto.flummi.request.IndicesExistsRequestBuilder;
import de.otto.flummi.util.HttpClientWrapper;

public class IndicesAdminClient {

    private HttpClientWrapper httpClient;

    public IndicesAdminClient(HttpClientWrapper httpClient) {
        this.httpClient = httpClient;
    }

    public CreateIndexRequestBuilder prepareCreate(String indexName) {
        return new CreateIndexRequestBuilder(httpClient, indexName);
    }

    public IndicesExistsRequestBuilder prepareExists(String indexName) {
        return new IndicesExistsRequestBuilder(httpClient, indexName);
    }

    public DeleteIndexRequestBuilder prepareDelete(String... indexNames) {
        return new DeleteIndexRequestBuilder(httpClient, indexNames);
    }
}
