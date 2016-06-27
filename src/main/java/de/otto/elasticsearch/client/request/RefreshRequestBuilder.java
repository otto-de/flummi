package de.otto.elasticsearch.client.request;

import com.ning.http.client.Response;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class RefreshRequestBuilder {
    private RoundRobinLoadBalancingHttpClient httpClient;
    private final String indexName;

    public static final Logger LOG = getLogger(RefreshRequestBuilder.class);

    public RefreshRequestBuilder(RoundRobinLoadBalancingHttpClient httpClient, String indexName) {
        this.httpClient = httpClient;
        this.indexName = indexName;
    }

    public void execute() {
        try {
            Response response = httpClient.preparePost("/" + indexName + "/_refresh").execute().get();
            if (response.getStatusCode() >= 300) {
                throw toHttpServerErrorException(response);
            }
            return;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
