package de.otto.flummi;

import de.otto.flummi.request.ClusterHealthRequestBuilder;
 import org.elasticsearch.client.RestClient;

public class ClusterAdminClient {
    private RestClient httpClient;

    public ClusterAdminClient(RestClient httpClient) {
        this.httpClient = httpClient;
    }

    public ClusterHealthRequestBuilder prepareHealth(String... indexNames) {
        return new ClusterHealthRequestBuilder(httpClient, indexNames);
    }
}
