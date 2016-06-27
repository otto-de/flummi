package de.otto.elasticsearch.client;

import de.otto.elasticsearch.client.request.ClusterHealthRequestBuilder;
import de.otto.elasticsearch.client.util.HttpClientWrapper;

public class ClusterAdminClient {
    private HttpClientWrapper httpClient;

    public ClusterAdminClient(HttpClientWrapper httpClient) {
        this.httpClient = httpClient;
    }

    public ClusterHealthRequestBuilder prepareHealth(String... indexNames) {
        return new ClusterHealthRequestBuilder(httpClient, indexNames);
    }
}
