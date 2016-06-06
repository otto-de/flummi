package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class CountRequestBuilder implements RequestBuilder<Long> {

    private final AsyncHttpClient client;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final String[] indices;
    private final Gson gson;
    private String[] types;

    public static final Logger LOG = getLogger(CountRequestBuilder.class);

    public CountRequestBuilder(AsyncHttpClient client, ImmutableList<String> hosts, int hostIndexOfNextRequest, String... indices) {
        this.client = client;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
        this.indices = indices;
        this.gson = new Gson();
    }

    public CountRequestBuilder setTypes(String... types) {
        this.types = types;
        return this;
    }

    @Override
    public Long execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            try {
                String url = RequestBuilderUtil.buildUrl(hosts.get(i), indices, types, "_count");
                Response response = client.prepareGet(url).execute().get();
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