package de.otto.flummi.request;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import de.otto.flummi.response.SearchResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static de.otto.flummi.RequestBuilderUtil.toHttpServerErrorException;
import static de.otto.flummi.request.GsonHelper.object;
import static de.otto.flummi.request.SearchRequestBuilder.parseResponse;
import static de.otto.flummi.response.SearchResponse.emptyResponse;

public class SearchScrollRequestBuilder implements RequestBuilder<SearchResponse> {
    private final Gson gson;
    private RestClient restClient;
    private String scrollId;
    private String scroll;

    public SearchScrollRequestBuilder(RestClient restClient) {
        this.restClient = restClient;
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
            StringEntity entity = new StringEntity(gson.toJson(requestBody));
            Response response = restClient.performRequest("POST", "/_search/scroll", Collections.emptyMap(), entity);

            //Did not find an entry
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 404) {
                return emptyResponse();
            }

            //Server Error
            if (statusCode >= 300) {
                throw toHttpServerErrorException(response);
            }

            JsonObject jsonResponse = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonObject.class);
            SearchResponse.Builder searchResponse = parseResponse(jsonResponse, null, null);

            return searchResponse.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }
}
