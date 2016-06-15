package de.otto.elasticsearch.client;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.request.ClusterHealthRequestBuilder;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;

public class ClusterAdminClient {
    private RoundRobinLoadBalancingHttpClient httpClient;

    public ClusterAdminClient(RoundRobinLoadBalancingHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public ClusterHealthRequestBuilder prepareHealth(String... indexNames) {
        return new ClusterHealthRequestBuilder(httpClient, indexNames);
    }
}
