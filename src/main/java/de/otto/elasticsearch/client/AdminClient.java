package de.otto.elasticsearch.client;

import de.otto.elasticsearch.client.util.HttpClientWrapper;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class AdminClient {

    public static final Logger LOG = getLogger(AdminClient.class);
    private HttpClientWrapper httpClient;

    public AdminClient(HttpClientWrapper httpClient) {
        this.httpClient = httpClient;
    }

    public IndicesAdminClient indices() {
        return new IndicesAdminClient(httpClient);
    }

    public ClusterAdminClient cluster() {
        return new ClusterAdminClient(httpClient);
    }
}
