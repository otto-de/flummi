package de.otto.elasticsearch.client;


import de.otto.elasticsearch.client.request.CreateIndexRequestBuilder;
import de.otto.elasticsearch.client.request.DeleteIndexRequestBuilder;
import de.otto.elasticsearch.client.request.IndicesExistsRequestBuilder;
import de.otto.elasticsearch.client.util.HttpClientWrapper;

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
