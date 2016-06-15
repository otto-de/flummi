package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;

import static org.slf4j.LoggerFactory.getLogger;

public class DeleteIndexRequestBuilder {
    private final RoundRobinLoadBalancingHttpClient httpClient;
    private final String indexName;

    public DeleteIndexRequestBuilder(RoundRobinLoadBalancingHttpClient httpClient, String indexName) {
        this.httpClient = httpClient;
        this.indexName = indexName;
    }

    public void execute() {
        try {
            Response response = httpClient.prepareDelete("/" + indexName).execute().get();
            if (response.getStatusCode() >= 300 && response.getStatusCode() != 404) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            return;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
