package de.otto.elasticsearch.client;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.request.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static de.otto.elasticsearch.client.util.CollectionUtils.toImmutableList;


public class ElasticSearchHttpClient {

    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private int hostIndexOfNextRequest = 0;
    private final Gson gson;

    public static final Logger LOG = LoggerFactory.getLogger(ElasticSearchHttpClient.class);

    public ElasticSearchHttpClient(AsyncHttpClient asyncHttpClient, String hosts) {
        this.asyncHttpClient = asyncHttpClient;
        this.gson = new Gson();
        this.hosts = Arrays.asList(hosts.split(",")).stream().map(String::trim).collect(toImmutableList());
    }

    private void nextHost() {
        hostIndexOfNextRequest = (hostIndexOfNextRequest + 1) % hosts.size();
    }

    public Optional<String> getIndexNameForAlias(String aliasName) {
        nextHost();
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), "_aliases");
            try {
                Response response = asyncHttpClient.prepareGet(url).execute().get();
                if (response.getStatusCode() != 200) {
                    throw toHttpServerErrorException(response);
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

    public void pointProductAliasToCurrentIndex(String aliasName, String indexName) throws InvalidElasticsearchResponseException {
        nextHost();
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), "_aliases");
            try {
                JsonArray actions = new JsonArray();
                actions.add(object("remove", object("index", "*", "alias", aliasName)));
                actions.add(object("add", object("index", indexName, "alias", aliasName)));
                JsonObject jsonObject = object("actions", actions);

                Response response = asyncHttpClient
                        .preparePost(url)
                        .setBody(gson.toJson(jsonObject))
                        .execute().get();
                if (response.getStatusCode()>=300) {
                    throw toHttpServerErrorException(response);
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

    public boolean indexExists(String indexName) {
        nextHost();
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), indexName);
            try {
                Response response = asyncHttpClient.prepareGet(url).execute().get();
                if (response.getStatusCode() != 200) {
                    return false;
                }
                String jsonString = response.getResponseBody();
                JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);
                return responseObject.entrySet().size() > 0;
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

    public boolean aliasExists(String aliasName) {
        nextHost();
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), "_aliases");
            try {
                Response response = asyncHttpClient.prepareGet(url).execute().get();
                if (response.getStatusCode() != 200) {
                    throw toHttpServerErrorException(response);
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

    public List<String> getAllIndexNames() {
        nextHost();
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), "_all");
            ImmutableList.Builder<String> indexNamesBuilder = ImmutableList.builder();
            try {
                Response response = asyncHttpClient.prepareGet(url).execute().get();
                if (response.getStatusCode() != 200) {
                    throw toHttpServerErrorException(response);
                }
                String jsonString = response.getResponseBody();
                JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);

                responseObject.entrySet().stream()
                        .forEach(e -> indexNamesBuilder.add(e.getKey()));
                return indexNamesBuilder.build();
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

    public boolean deleteIndex(List<String> indexNamesToDelete) throws InvalidElasticsearchResponseException {
        nextHost();
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            if (indexNamesToDelete.isEmpty()) {
                return true;
            }
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), indexNamesToDelete.toArray(new String[indexNamesToDelete.size()]), null, null);
            try {
                Response response = asyncHttpClient.prepareDelete(url).execute().get();
                if (response.getStatusCode() != 200) {
                    throw toHttpServerErrorException(response);
                }
                String jsonString = response.getResponseBody();
                JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);
                if (responseObject.has("acknowledged")) {
                    return responseObject.get("acknowledged").getAsBoolean();
                } else {
                    throw new InvalidElasticsearchResponseException("Response does not contain field 'acknowledged'");
                }
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

    public JsonObject getIndexSettings() {
        nextHost();
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            String url = RequestBuilderUtil.buildUrl(hosts.get(i), "_all/_settings");
            try {
                Response response = asyncHttpClient.prepareGet(url).execute().get();
                if (response.getStatusCode() != 200) {
                    throw toHttpServerErrorException(response);
                }
                String jsonString = null;
                try {
                    jsonString = response.getResponseBody();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return gson.fromJson(jsonString, JsonObject.class);
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

    public void refreshIndex(final String indexName) {
        nextHost();
        new RefreshRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indexName).execute();
    }

    public SearchRequestBuilder prepareSearch(String... indices) {
        nextHost();
        return new SearchRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indices);
    }

    public CountRequestBuilder prepareCount(String... indices) {
        nextHost();
        return new CountRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indices);
    }

    public BulkRequestBuilder prepareBulk() {
        nextHost();
        return new BulkRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest);
    }

    public GetRequestBuilder prepareGet(String indexName, String documentType, String id) {
        nextHost();
        return new GetRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indexName, documentType, id);
    }

    public DeleteIndexRequestBuilder prepareDeleteByName(String indexName) {
        nextHost();
        return new DeleteIndexRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indexName);
    }

    public DeleteRequestBuilder prepareDelete() {
        nextHost();
        return new DeleteRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest);
    }

    public MultiGetRequestBuilder prepareMultiGet(String[] indices) {
        nextHost();
        return new MultiGetRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest, indices);
    }

    public IndexRequestBuilder prepareIndex() {
        nextHost();
        return new IndexRequestBuilder(asyncHttpClient, hosts, hostIndexOfNextRequest);
    }

    public AdminClient admin() {
        nextHost();
        return new AdminClient(asyncHttpClient, hosts, hostIndexOfNextRequest);
    }
}
