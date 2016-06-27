package de.otto.elasticsearch.client.request;

import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.util.HttpClientWrapper;

import java.util.concurrent.ExecutionException;

public class DeleteIndexRequestBuilder {
    private final HttpClientWrapper httpClient;
    private final String indexName;

    public DeleteIndexRequestBuilder(HttpClientWrapper httpClient, String indexName) {
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
