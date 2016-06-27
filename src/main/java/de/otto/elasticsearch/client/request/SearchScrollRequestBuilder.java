package de.otto.elasticsearch.client.request;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.response.SearchResponse;
import de.otto.elasticsearch.client.util.HttpClientWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static de.otto.elasticsearch.client.request.SearchRequestBuilder.parseResponse;
import static de.otto.elasticsearch.client.response.SearchResponse.emptyResponse;

public class SearchScrollRequestBuilder implements RequestBuilder<SearchResponse> {
    private final Gson gson;
    private HttpClientWrapper httpClient;
    private String scrollId;
    private String scroll;

    public SearchScrollRequestBuilder(HttpClientWrapper httpClient) {
        this.httpClient = httpClient;
        gson = new Gson();
    }

    public SearchScrollRequestBuilder setScrollId(String scrollId) {
        this.scrollId = scrollId;
        return this;
    }

    public SearchScrollRequestBuilder setScroll(String scroll) {
        this.scroll = scroll;
        return this;
    }

    @Override
    public SearchResponse execute() {
        JsonObject requestBody = object(
                "scroll_id", scrollId,
                "scroll", scroll
        );
        try {
            Response response = httpClient.preparePost("/_search/scroll")
                    .setBody(gson.toJson(requestBody))
                    .execute()
                    .get();

            //Did not find an entry
            if (response.getStatusCode() == 404) {
                return emptyResponse();
            }

            //Server Error
            if (response.getStatusCode() >= 300) {
                throw toHttpServerErrorException(response);
            }

            JsonObject jsonResponse = gson.fromJson(response.getResponseBody(), JsonObject.class);
            SearchResponse.Builder searchResponse = parseResponse(jsonResponse, null, null);

            return searchResponse.build();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }
}
