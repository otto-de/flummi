package de.otto.flummi.request;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import de.otto.flummi.InvalidElasticsearchResponseException;
import de.otto.flummi.RequestBuilderUtil;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.Response;
import org.slf4j.Logger;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;

import static de.otto.flummi.request.RequestConstants.APPL_JSON;
import static de.otto.flummi.request.RequestConstants.CONTENT_TYPE;
import static org.slf4j.LoggerFactory.getLogger;

public class CreateIndexRequestBuilder implements RequestBuilder<Void> {
    private final Gson gson;
    private final String indexName;
    private JsonObject settings;
    private JsonObject mappings;
    private final HttpClientWrapper httpClient;

    public static final Logger LOG = getLogger(CreateIndexRequestBuilder.class);

    public CreateIndexRequestBuilder(HttpClientWrapper httpClient, String indexName) {
        this.httpClient = httpClient;
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

    public Void execute() {

        JsonObject jsonObject = new JsonObject();
        if (settings != null) {
            jsonObject.add("settings", settings);
        }
        if (mappings != null) {
            jsonObject.add("mappings", mappings);
        }
        try {
            Response response = httpClient.preparePut("/" + indexName).setBody(jsonObject.toString()).setCharset(Charset.forName("UTF-8"))
                    .addHeader(CONTENT_TYPE, APPL_JSON)
                    .execute().get();
            if (response.getStatusCode() >= 300) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            String jsonString = response.getResponseBody();
            JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);
            if (!responseObject.has("acknowledged") || !responseObject.get("acknowledged").getAsBoolean()) {
                throw new InvalidElasticsearchResponseException("Invalid reply from Elastic Search: " + jsonString);
            }
            return null;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
