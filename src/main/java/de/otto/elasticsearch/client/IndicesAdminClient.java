package de.otto.elasticsearch.client;


import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.request.CreateIndexRequestBuilder;
import de.otto.elasticsearch.client.request.DeleteIndexRequestBuilder;
import de.otto.elasticsearch.client.request.IndicesExistsRequestBuilder;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;

public class IndicesAdminClient {

    private RoundRobinLoadBalancingHttpClient httpClient;

    public IndicesAdminClient(RoundRobinLoadBalancingHttpClient httpClient) {

        this.httpClient = httpClient;
    }

    public CreateIndexRequestBuilder prepareCreate(String indexName) {
        return new CreateIndexRequestBuilder(httpClient, indexName);
    }

    public IndicesExistsRequestBuilder prepareExists(String indexName) {
        return new IndicesExistsRequestBuilder(httpClient, indexName);
    }

    public DeleteIndexRequestBuilder prepareDelete(String indexName) {
        return new DeleteIndexRequestBuilder(httpClient, indexName);
    }
}
