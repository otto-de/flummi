package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;

import static org.slf4j.LoggerFactory.getLogger;

public class DeleteIndexRequestBuilder {
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final String indexName;
    private final AsyncHttpClient asyncHttpClient;

    public static final Logger LOG = getLogger(DeleteIndexRequestBuilder.class);

    public DeleteIndexRequestBuilder(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest, String indexName) {
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
        this.indexName = indexName;
        this.asyncHttpClient = asyncHttpClient;
    }

    public void execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), indexName);

            try {
                Response response = asyncHttpClient.prepareDelete(url).execute().get();
                if (response.getStatusCode() >= 300 && response.getStatusCode() != 404) {
                    throw RequestBuilderUtil.toHttpServerErrorException(response);
                }
                return;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                if (i == ((hostIndexOfNextRequest - 1) % hosts.size())) {
                    LOG.warn("Could not connect to host '" + hosts.get(i) + "'");
                    throw new RuntimeException(e);
                } else {
                    LOG.warn("Could not connect to host '" + hosts.get(i) + "'");
                }
            }
            count++;
        }
        throw new RuntimeException("Could not connect to cluster");
    }
}
