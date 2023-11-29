package de.otto.flummi.request;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.otto.flummi.InvalidElasticsearchResponseException;
import de.otto.flummi.bulkactions.BulkActionBuilder;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static de.otto.flummi.RequestBuilderUtil.toHttpServerErrorException;
import static de.otto.flummi.request.RequestConstants.APPL_JSON;
import static de.otto.flummi.request.RequestConstants.CONTENT_TYPE;
import static org.slf4j.LoggerFactory.getLogger;

public class BulkRequestBuilder implements RequestBuilder<Void> {
    private final Gson gson;
    private List<BulkActionBuilder> actions = Collections.synchronizedList(new ArrayList());

    public static final Logger LOG = getLogger(BulkRequestBuilder.class);
    private HttpClientWrapper httpClient;

    public BulkRequestBuilder(HttpClientWrapper httpClient) {
        this.httpClient = httpClient;
        this.gson = new Gson();

    }

    public BulkRequestBuilder add(BulkActionBuilder action) {
        this.actions.add(action);
        return this;
    }

    @Override
    public Void execute() {
        try {
            if (actions.isEmpty()) {
                return null;
            }

            StringBuilder postBody = new StringBuilder();

            for (BulkActionBuilder action : this.actions) {
                postBody.append(action.toBulkRequestAction()).append("\n");
            }

	        final BoundRequestBuilder boundRequestBuilder = httpClient
	                .preparePost("/_bulk")
.addHeader(CONTENT_TYPE,APPL_JSON)
                    .setBody(postBody.toString())
	                .setCharset(Charset.forName("UTF-8"));

            Response response = boundRequestBuilder.execute().get();
            if (response.getStatusCode() >= 300) {
                throw toHttpServerErrorException(response);
            }
            String jsonString = response.getResponseBody();
            JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);

            String errors = responseObject.get("errors").getAsString();

            if (("true").equals(errors)) {
                LOG.error("Error in bulk request detected {}", jsonString);
                boolean foundError = false;
                JsonArray items = responseObject.get("items") != null ? responseObject.get("items").getAsJsonArray() : new JsonArray();
                for (JsonElement jsonElement : items) {
                    JsonElement updateField = jsonElement.getAsJsonObject().get("update");
                    if (updateField != null) {
                        final JsonElement status = updateField.getAsJsonObject().get("status");
                        final JsonElement error = updateField.getAsJsonObject().get("error");
                        if (status != null && status.getAsInt() != 404 && error != null && error.isJsonPrimitive() && !error.getAsString().isEmpty()) {
                            foundError = true;
                        }
                    } else {
                        for (Map.Entry<String, JsonElement> opElement : jsonElement.getAsJsonObject().entrySet()) {
                            JsonObject opObject = opElement.getValue().getAsJsonObject();
                            JsonElement errorObj = opObject.get("error");
                            if (errorObj != null && errorObj.isJsonPrimitive() && !errorObj.getAsString().isEmpty()) {
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public int size() {
        return actions.size();
    }
}
