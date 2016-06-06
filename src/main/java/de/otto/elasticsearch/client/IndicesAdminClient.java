package de.otto.elasticsearch.client;


import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.request.CreateIndexRequestBuilder;
import de.otto.elasticsearch.client.request.DeleteIndexRequestBuilder;
import de.otto.elasticsearch.client.request.IndicesExistsRequestBuilder;

public class IndicesAdminClient {

    final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;

    public IndicesAdminClient(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
    }

    public CreateIndexRequestBuilder prepareCreate(String indexName) {
        return new CreateIndexRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indexName);
    }

    public IndicesExistsRequestBuilder prepareExists(String indexName) {
        return new IndicesExistsRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indexName);
    }

    public DeleteIndexRequestBuilder prepareDelete(String indexName) {
        return new DeleteIndexRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indexName);
    }
}
