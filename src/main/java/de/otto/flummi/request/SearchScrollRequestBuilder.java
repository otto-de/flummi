package de.otto.flummi.request;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import de.otto.flummi.response.SearchResponse;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.Response;

import java.util.concurrent.ExecutionException;

import static de.otto.flummi.RequestBuilderUtil.toHttpServerErrorException;
import static de.otto.flummi.request.GsonHelper.object;
import static de.otto.flummi.request.RequestConstants.APPL_JSON;
import static de.otto.flummi.request.RequestConstants.CONTENT_TYPE;
import static de.otto.flummi.request.SearchRequestBuilder.parseResponse;
import static de.otto.flummi.response.SearchResponse.emptyResponse;

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
                    .addHeader(CONTENT_TYPE, APPL_JSON)
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
        }

    }
}
