package de.otto.flummi.request;

import org.elasticsearch.client.Response;
import de.otto.flummi.RequestBuilderUtil;
 import org.elasticsearch.client.RestClient;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class DeleteIndexRequestBuilder implements RequestBuilder<Void> {
    private final RestClient httpClient;
    private final String[] indexNames;

    public DeleteIndexRequestBuilder(RestClient httpClient, Stream<String> indexNames) {
        this.httpClient = httpClient;
        this.indexNames = toArray(indexNames);
    }

    public Void execute() {
        if (indexNames.length == 0) {
            throw new RuntimeException("index names are missing");
        }
        try {
            String url = RequestBuilderUtil.buildUrl(indexNames, null, null);
            Response response = httpClient.prepareDelete(url).execute().get();
            if (response.getStatusCode() >= 300 && response.getStatusCode() != 404) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            return null;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private String[] toArray(Stream<String> indexNames) {
        return indexNames == null ? null : indexNames.collect(toList()).toArray(new String[] {});
    }
}
