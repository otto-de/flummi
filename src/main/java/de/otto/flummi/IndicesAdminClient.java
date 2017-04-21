package de.otto.flummi;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import de.otto.flummi.request.*;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static de.otto.flummi.request.GsonHelper.object;
import static java.util.stream.Collectors.toList;

public class IndicesAdminClient {

    private final RestClient restClient;
    private final Gson gson = new Gson();

    public IndicesAdminClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public CreateIndexRequestBuilder prepareCreate(String indexName) {
        return new CreateIndexRequestBuilder(restClient, indexName);
    }

    public IndicesExistsRequestBuilder prepareExists(String indexName) {
        return new IndicesExistsRequestBuilder(restClient, indexName);
    }

    public DeleteIndexRequestBuilder prepareDelete(Stream<String> indexNameSupplier) {
        return new DeleteIndexRequestBuilder(restClient, indexNameSupplier);
    }

    public DeleteIndexRequestBuilder prepareDelete(String... indexNames) {
        return new DeleteIndexRequestBuilder(restClient, Stream.of(indexNames));
    }

    public RefreshRequestBuilder prepareRefresh(String indexName) {
        return new RefreshRequestBuilder(restClient, indexName);
    }

    public ForceMergeRequestBuilder forceMerge(String indexName) {
        return new ForceMergeRequestBuilder(restClient, indexName);
    }

    public JsonObject getIndexSettings() {
        try {
            Response response = restClient.prepareGet("/_all/_settings").execute().get();
            if (response.getStatusCode() != 200) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            String jsonString = null;
            try {
                jsonString = response.getResponseBody();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return gson.fromJson(jsonString, JsonObject.class);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getAllIndexNames() {
        try {
            Response response = restClient.prepareGet("/_all").execute().get();
            if (response.getStatusCode() != 200) {
                throw RequestBuilderUtil.toHttpServerErrorException(response);
            }
            String jsonString = response.getResponseBody();
            JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);

            return responseObject.entrySet().stream()
                    .map(Map.Entry::getKey)
                    .collect(toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<String> getIndexNameForAlias(String aliasName) {
        try {
            Response response = restClient.prepareGet("/_aliases").execute().get();
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

            Response response = restClient
                    .preparePost("/_aliases")
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean aliasExists(String aliasName) {
        try {
            Response response = restClient.prepareGet("/_aliases").execute().get();
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
