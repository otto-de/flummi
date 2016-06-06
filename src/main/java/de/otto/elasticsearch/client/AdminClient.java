package de.otto.elasticsearch.client;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class AdminClient {
    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;

    public static final Logger LOG = getLogger(AdminClient.class);

    public AdminClient(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
    }

    public IndicesAdminClient indices() {
        return new IndicesAdminClient(asyncHttpClient, hosts, hostIndexOfNextRequest);
    }

    public ClusterAdminClient cluster() {
        return new ClusterAdminClient(asyncHttpClient, hosts, hostIndexOfNextRequest);
    }
}
