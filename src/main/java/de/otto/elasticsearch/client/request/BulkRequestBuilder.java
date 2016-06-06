package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.InvalidElasticsearchResponseException;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.bulkactions.BulkActionBuilder;
import de.otto.elasticsearch.client.util.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Iterables.isEmpty;
import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class BulkRequestBuilder implements RequestBuilder<Void> {
    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final Gson gson;
    private List<BulkActionBuilder> actions = new ArrayList();

    public static final Logger LOG = getLogger(BulkRequestBuilder.class);

    public BulkRequestBuilder(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
        this.gson = new Gson();
    }

    public BulkRequestBuilder add(BulkActionBuilder action) {
        this.actions.add(action);
        return this;
    }

    @Override
    public Void execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            try {
                if (isEmpty(actions)) {
                    return null;
                }

                String url = RequestBuilderUtil.buildUrl(hosts.get(i), "_bulk");
                StringBuilder postBody = new StringBuilder();

                for (BulkActionBuilder action : this.actions) {
                    postBody.append(action.toBulkRequestAction()).append("\n");
                }

                final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = asyncHttpClient
                        .preparePost(url)
                        .setBody(postBody.toString())
                        .setBodyEncoding("UTF-8");

                Response response = boundRequestBuilder.execute().get();
                if (response.getStatusCode() >= 300) {
                    throw toHttpServerErrorException(response);
                }
                String jsonString = response.getResponseBody();
                JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);

                String errors = responseObject.get("errors").getAsString();

                if (("true").equals(errors)) {
                    boolean foundError = false;
                    JsonArray items = responseObject.get("items") != null ? responseObject.get("items").getAsJsonArray() : new JsonArray();
                    for (JsonElement jsonElement : items) {
                        JsonElement updateField = jsonElement.getAsJsonObject().get("update");
                        if (updateField != null) {
                            final JsonElement status = updateField.getAsJsonObject().get("status");
                            final JsonElement error = updateField.getAsJsonObject().get("error");
                            if (status != null && status.getAsInt() != 404 && error != null && !StringUtils.isEmpty(error.getAsString())) {
                                foundError = true;
                            }
                        } else {
                            for (Map.Entry<String, JsonElement> opElement : jsonElement.getAsJsonObject().entrySet()) {
                                JsonObject opObject = opElement.getValue().getAsJsonObject();
                                if (opObject != null && opObject.get("error") != null && !StringUtils.isEmpty(opObject.get("error").getAsString())) {
                                    foundError = true;
                                }
                            }
                        }
                    }

                    if (foundError) {
                        throw new InvalidElasticsearchResponseException("Response contains errors': " + jsonString);
                    }
                }
                return null;
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

    public int size() {
        return actions.size();
    }
}
