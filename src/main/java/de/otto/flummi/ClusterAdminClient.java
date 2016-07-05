package de.otto.flummi;

import de.otto.flummi.request.ClusterHealthRequestBuilder;
import de.otto.flummi.util.HttpClientWrapper;

public class ClusterAdminClient {
    private HttpClientWrapper httpClient;

    public ClusterAdminClient(HttpClientWrapper httpClient) {
        this.httpClient = httpClient;
    }

    public ClusterHealthRequestBuilder prepareHealth(String... indexNames) {
        return new ClusterHealthRequestBuilder(httpClient, indexNames);
    }
}
