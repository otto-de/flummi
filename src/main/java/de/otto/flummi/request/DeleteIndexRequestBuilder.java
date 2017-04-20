package de.otto.flummi.request;

import com.ning.http.client.Response;
import de.otto.flummi.RequestBuilderUtil;
import de.otto.flummi.util.HttpClientWrapper;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class DeleteIndexRequestBuilder implements RequestBuilder<Void> {
    private final HttpClientWrapper httpClient;
    private final String[] indexNames;

    public DeleteIndexRequestBuilder(HttpClientWrapper httpClient, Stream<String> indexNames) {
        this.httpClient = httpClient;
        this.indexNames = toArray(indexNames);
    }

    public Void execute() {
        if (indexNames.length == 0) {
            return null;
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
