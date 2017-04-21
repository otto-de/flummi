package de.otto.flummi.request;


import org.elasticsearch.client.Response;
import de.otto.flummi.response.HttpServerErrorException;
 import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;

import static org.slf4j.LoggerFactory.getLogger;

public class IndicesExistsRequestBuilder implements RequestBuilder<Boolean> {
    private final String indexName;

    public static final Logger LOG = getLogger(IndicesExistsRequestBuilder.class);
    private RestClient httpClient;

    public IndicesExistsRequestBuilder(RestClient httpClient, String indexName) {
        this.indexName = indexName;
        this.httpClient = httpClient;
    }

    public Boolean execute() {
        try {
            Response response = httpClient.prepareHead("/" + indexName).execute().get();
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
            throw new RuntimeException(e);
        }
    }
}
