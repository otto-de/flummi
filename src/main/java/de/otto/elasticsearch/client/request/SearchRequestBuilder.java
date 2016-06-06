package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.*;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.SortOrder;
import de.otto.elasticsearch.client.aggregations.AggregationBuilder;
import de.otto.elasticsearch.client.query.QueryBuilder;
import de.otto.elasticsearch.client.response.Aggregation;
import de.otto.elasticsearch.client.response.SearchHit;
import de.otto.elasticsearch.client.response.SearchHits;
import de.otto.elasticsearch.client.response.SearchResponse;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class SearchRequestBuilder implements RequestBuilder<SearchResponse> {
    private static final JsonObject EMPTY_JSON_OBJECT = new JsonObject();

    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final String[] indices;
    private final Gson gson;
    private String[] types;
    private JsonObject query;
    private Integer from;
    private Integer size;
    private Integer timeoutMillis;
    private JsonArray sorts;
    private JsonArray fields;
    private QueryBuilder postFilter;
    private Map<String, AggregationBuilder> aggregations;

    public static final Logger LOG = getLogger(SearchRequestBuilder.class);

    public SearchRequestBuilder(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest, String... indices) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
        this.indices = indices;
        this.gson = new Gson();
    }

    public SearchRequestBuilder setTypes(String... types) {
        this.types = types;
        return this;
    }

    public SearchRequestBuilder setQuery(JsonObject query) {
        this.query = query;
        return this;
    }

    public SearchRequestBuilder addAggregation(String name, AggregationBuilder aggregationBuilder) {
        if (aggregations == null) {
            aggregations = new HashMap<>();
        }
        aggregations.put(name, aggregationBuilder);
        return this;
    }

    public SearchRequestBuilder addSort(String key, SortOrder order) {
        if (sorts == null) {
            sorts = new JsonArray();
        }
        JsonObject element = new JsonObject();
        JsonObject orderObj = new JsonObject();
        element.add(key, orderObj);
        orderObj.add("order", new JsonPrimitive(order.toString()));
        sorts.add(element);
        return this;
    }

    public SearchRequestBuilder setFrom(int from) {
        this.from = from;
        return this;
    }

    public SearchRequestBuilder setSize(int size) {
        this.size = size;
        return this;
    }

    public SearchRequestBuilder addField(String fieldName) {
        if (fields == null) {
            fields = new JsonArray();
        }
        fields.add(new JsonPrimitive(fieldName));
        return this;
    }

    public SearchRequestBuilder setTimeoutMillis(Integer timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    @Override
    public SearchResponse execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            try {
                String url = RequestBuilderUtil.buildUrl(hosts.get(i), indices, types, "_search");
                JsonObject body = new JsonObject();
                if (query != null) {
                    body.add("query", query);
                }
                if (fields != null) {
                    body.add("fields", fields);
                }
                if (from != null) {
                    body.add("from", new JsonPrimitive(from));
                }
                if (size != null) {
                    body.add("size", new JsonPrimitive(size));
                }
                if (sorts != null) {
                    body.add("sort", sorts);
                }
                if (postFilter != null) {
                    body.add("post_filter", postFilter.build());
                }
                if (aggregations != null) {
                    JsonObject jsonObject = new JsonObject();
                    aggregations.entrySet()
                            .stream()
                            .forEach(a ->
                                    jsonObject.add(a.getKey(), a.getValue().build()));
                    body.add("aggregations", jsonObject);
                }
                AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = asyncHttpClient
                        .preparePost(url)
                        .setBodyEncoding("UTF-8");
                if (timeoutMillis != null) {
                    boundRequestBuilder.setRequestTimeout(timeoutMillis);
                }

                Response response = boundRequestBuilder.setBody(gson.toJson(body))
                        .execute()
                        .get();

                //Did not find an entry
                if (response.getStatusCode() == 404) {
                    return new SearchResponse(0, new SearchHits(0L, 0F, ImmutableList.of()), ImmutableMap.of());
                }

                //Server Error
                if (response.getStatusCode() >= 300) {
                    throw toHttpServerErrorException(response);
                }

                JsonObject jsonObject = gson.fromJson(response.getResponseBody(), JsonObject.class);
                long took = jsonObject.get("took").getAsLong();
                JsonObject hits = jsonObject.get("hits").getAsJsonObject();
                long totalHits = hits.get("total").getAsLong();
                JsonElement max_score = hits.get("max_score");
                Float maxScore = max_score.isJsonPrimitive() ? max_score.getAsFloat() : null;
                JsonArray hitsArray = hits.get("hits").getAsJsonArray();


                final Map<String, Aggregation> responseAggregations = new HashMap<>();
                JsonElement aggregationsJsonElement = jsonObject.get("aggregations");
                if (aggregationsJsonElement != null) {
                    final JsonObject aggregationsJsonObject = aggregationsJsonElement.getAsJsonObject();

                    aggregations.entrySet().stream().forEach(a -> {
                        JsonElement aggreagationElement = aggregationsJsonObject.get(a.getKey());
                        if (aggreagationElement != null) {
                            Aggregation aggregation = a.getValue().parseResponse(aggreagationElement.getAsJsonObject());
                            responseAggregations.put(a.getKey(), aggregation);
                        }
                    });
                }

                List<SearchHit> searchHit = new ArrayList<>();
                for (JsonElement element : hitsArray) {
                    JsonObject asJsonObject = element.getAsJsonObject();
                    JsonElement scoreElem = asJsonObject.get("_score");
                    Float score = scoreElem.isJsonNull() ? null : scoreElem.getAsFloat();
                    String id = asJsonObject.get("_id").getAsString();
                    JsonElement source = asJsonObject.get("_source");
                    JsonElement hitFields = asJsonObject.get("fields");
                    SearchHit hit = new SearchHit(id,
                            source != null ? source.getAsJsonObject() : null,
                            hitFields != null ? hitFields.getAsJsonObject() : EMPTY_JSON_OBJECT,
                            score);
                    searchHit.add(hit);
                }
                SearchHits searchHits = new SearchHits(totalHits, maxScore, searchHit);
                return new SearchResponse(took, searchHits, responseAggregations);
            } catch (IOException e) {
                throw new RuntimeException(e);
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

    public SearchRequestBuilder setPostFilter(QueryBuilder postFilter) {
        this.postFilter = postFilter;
        return this;
    }
}
