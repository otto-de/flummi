package de.otto.elasticsearch.client.request;


import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;

import static org.slf4j.LoggerFactory.getLogger;

public class IndicesExistsRequestBuilder {
    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final String indexName;

    public static final Logger LOG = getLogger(IndicesExistsRequestBuilder.class);

    public IndicesExistsRequestBuilder(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest, String indexName) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
        this.indexName = indexName;
    }

    public boolean execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), indexName);
            try {
                Response response = asyncHttpClient.prepareHead(url).execute().get();
                int statusCode = response.getStatusCode();
                if (statusCode >= 300 && response.getStatusCode() != 404) {
                    throw new HttpServerErrorException(response.getStatusCode(), response.getStatusText(), response.getResponseBody());
                }
                return statusCode < 300;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
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
