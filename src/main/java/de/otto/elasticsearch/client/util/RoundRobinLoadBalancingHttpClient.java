package de.otto.elasticsearch.client.util;

import com.ning.http.client.AsyncHttpClient;

import java.util.List;

import static java.lang.Math.abs;

public class RoundRobinLoadBalancingHttpClient {

    private final int numHosts;
    private final AsyncHttpClient asyncHttpClient;
    private final List<String> hosts;
    int roundRobinCounter;

    public RoundRobinLoadBalancingHttpClient(AsyncHttpClient asyncHttpClient, List<String> hosts) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.numHosts = hosts.size();
    }

    public AsyncHttpClient.BoundRequestBuilder prepareGet(String url) {
        return asyncHttpClient.prepareGet(nextHostName() + url);
    }

    public AsyncHttpClient.BoundRequestBuilder preparePost(String url) {
        return asyncHttpClient.preparePost(nextHostName() + url);
    }

    public AsyncHttpClient.BoundRequestBuilder preparePut(String url) {
        return asyncHttpClient.preparePut(nextHostName() + url);
    }

    public AsyncHttpClient.BoundRequestBuilder prepareDelete(String url) {
        return asyncHttpClient.prepareDelete(nextHostName() + url);
    }

    private String nextHostName() {
        return hosts.get(abs((roundRobinCounter++) % numHosts));
    }

    public AsyncHttpClient.BoundRequestBuilder prepareHead(String url) {
        return asyncHttpClient.prepareHead(nextHostName() + url);
    }
}
