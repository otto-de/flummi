package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.InvalidElasticsearchResponseException;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;

import static org.slf4j.LoggerFactory.getLogger;

public class CreateIndexRequestBuilder {
    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final Gson gson;
    private final String indexName;
    private JsonObject settings;
    private JsonObject mappings;

    public static final Logger LOG = getLogger(CreateIndexRequestBuilder.class);

    public CreateIndexRequestBuilder(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest, String indexName) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
        this.indexName = indexName;
        this.gson = new Gson();
    }

    public CreateIndexRequestBuilder setSettings(JsonObject settings) {
        this.settings = settings;
        return this;
    }

    public CreateIndexRequestBuilder setMappings(JsonObject mappings) {
        this.mappings = mappings;
        return this;
    }

    public void execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), indexName);

            JsonObject jsonObject = new JsonObject();
            if (settings != null) {
                jsonObject.add("settings", settings);
            }
            if (mappings != null) {
                jsonObject.add("mappings", mappings);
            }
            try {
                Response response = asyncHttpClient.preparePut(url).setBody(jsonObject.toString()).setBodyEncoding("UTF-8").execute().get();
                if (response.getStatusCode() >= 300) {
                    throw RequestBuilderUtil.toHttpServerErrorException(response);
                }
                String jsonString = response.getResponseBody();
                JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);
                if (!responseObject.has("acknowledged") || !responseObject.get("acknowledged").getAsBoolean()) {
                    throw new InvalidElasticsearchResponseException("Invalid reply from Elastic Search: " + jsonString);
                }
                return;
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
