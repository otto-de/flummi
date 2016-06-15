package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class CountRequestBuilder implements RequestBuilder<Long> {

    private final String[] indices;
    private final Gson gson;
    private String[] types;

    public static final Logger LOG = getLogger(CountRequestBuilder.class);
    private RoundRobinLoadBalancingHttpClient httpClient;

    public CountRequestBuilder(RoundRobinLoadBalancingHttpClient httpClient, String... indices) {
        this.httpClient = httpClient;
        this.indices = indices;
        this.gson = new Gson();
    }

    public CountRequestBuilder setTypes(String... types) {
        this.types = types;
        return this;
    }

    @Override
    public Long execute() {
        try {
            String url = RequestBuilderUtil.buildUrl(indices, types, "_count");
            Response response = httpClient.prepareGet(url).execute().get();
            if (response.getStatusCode() >= 300) {
                throw toHttpServerErrorException(response);
            }
            String jsonString = response.getResponseBody();
            JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);
            return responseObject.get("count").getAsLong();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}