package de.otto.flummi;


import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import de.otto.flummi.request.*;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.Response;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static de.otto.flummi.request.GsonHelper.object;
import static de.otto.flummi.request.RequestConstants.APPL_JSON;
import static de.otto.flummi.request.RequestConstants.CONTENT_TYPE;
import static java.util.stream.Collectors.toList;

public class IndicesAdminClient {

    private HttpClientWrapper httpClient;
    private Gson gson = new Gson();

    public IndicesAdminClient(HttpClientWrapper httpClient) {
        this.httpClient = httpClient;
    }

    public CreateIndexRequestBuilder prepareCreate(String indexName) {
        return new CreateIndexRequestBuilder(httpClient, indexName);
    }

    public IndicesExistsRequestBuilder prepareExists(String indexName) {
        return new IndicesExistsRequestBuilder(httpClient, indexName);
    }

    public DeleteIndexRequestBuilder prepareDelete(Stream<String> indexNameSupplier) {
        return new DeleteIndexRequestBuilder(httpClient, indexNameSupplier);
    }

    public DeleteIndexRequestBuilder prepareDelete(String... indexNames) {
        return new DeleteIndexRequestBuilder(httpClient, Stream.of(indexNames));
    }

    public RefreshRequestBuilder prepareRefresh(String indexName) {
        return new RefreshRequestBuilder(httpClient, indexName);
    }

    public ForceMergeRequestBuilder forceMerge(String indexName) {
        return new ForceMergeRequestBuilder(httpClient, indexName);
    }

    public JsonObject getIndexSettings() {
        try {

            Response response = httpClient.prepareGet("/_all/_settings")
                    .addHeader(CONTENT_TYPE, APPL_JSON)
                    .execute().get();
            if (response.getStatusCode() != 200) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            String jsonString = null;
            jsonString = response.getResponseBody();
            return gson.fromJson(jsonString, JsonObject.class);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public JsonObject getIndexMapping(String indexName) {
        try {
            Response response = httpClient.prepareGet("/" + indexName + "/_mapping")
                    .addHeader("Content-Type", "application/json")
                    .execute().get();
            if (response.getStatusCode() != 200) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            String jsonString = null;
            jsonString = response.getResponseBody();
            return gson.fromJson(jsonString, JsonObject.class);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getAllIndexNames() {
        try {
            Response response = httpClient.prepareGet("/_all")
                    .addHeader(CONTENT_TYPE, APPL_JSON)
                    .execute().get();
            if (response.getStatusCode() != 200) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            String jsonString = response.getResponseBody();
            JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);

            return responseObject.entrySet().stream()
                    .map(Map.Entry::getKey)
                    .collect(toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<String> getIndexNameForAlias(String aliasName) {
        try {
            Response response = httpClient.prepareGet("/_aliases")
                    .addHeader(CONTENT_TYPE, APPL_JSON)
                    .execute().get();
            if (response.getStatusCode() != 200) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            String jsonString = response.getResponseBody();

            return gson.fromJson(jsonString, JsonObject.class).entrySet().stream()
                    .filter(e -> (e.getValue() != null
                            && e.getValue().isJsonObject()
                            && e.getValue().getAsJsonObject().get("aliases") != null
                            && e.getValue().getAsJsonObject().get("aliases").isJsonObject()
                            && e.getValue().getAsJsonObject().get("aliases").getAsJsonObject().has(aliasName)))
                    .map(e -> e.getKey())
                    .findFirst();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void pointAliasToCurrentIndex(String aliasName, String indexName) {
        try {
            JsonArray actions = new JsonArray();
            actions.add(object("remove", object("index", "*", "alias", aliasName)));
            actions.add(object("add", object("index", indexName, "alias", aliasName)));
            JsonObject jsonObject = object("actions", actions);

            Response response = httpClient
                    .preparePost("/_aliases")
                    .addHeader(CONTENT_TYPE, APPL_JSON)
                    .setBody(gson.toJson(jsonObject))
                    .execute().get();
            if (response.getStatusCode() >= 300) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            JsonObject responseObject = gson.fromJson(response.getResponseBody(), JsonObject.class);
            if (!responseObject.has("acknowledged")) {
                throw new InvalidElasticsearchResponseException("Response does not contain field 'acknowledged': " + responseObject);
            } else {
                if (!responseObject.get("acknowledged").getAsBoolean()) {
                    throw new RuntimeException("Pointing product alias to current index not acknowledged");
                }
            }
            return;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean aliasExists(String aliasName) {
        try {
            Response response = httpClient.prepareGet("/_aliases")
                    .addHeader(CONTENT_TYPE, APPL_JSON)
                    .execute().get();
            if (response.getStatusCode() != 200) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            String jsonString = response.getResponseBody();
            JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);

            return responseObject.entrySet().stream().filter(e ->
                    (e.getValue() != null
                            && e.getValue().isJsonObject()
                            && e.getValue().getAsJsonObject().get("aliases") != null
                            && e.getValue().getAsJsonObject().get("aliases").isJsonObject()
                            && e.getValue().getAsJsonObject().get("aliases").getAsJsonObject().has(aliasName)))
                    .count() > 0;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
