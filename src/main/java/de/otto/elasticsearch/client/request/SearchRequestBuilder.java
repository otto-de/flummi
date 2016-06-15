package de.otto.elasticsearch.client.request;

import com.google.gson.*;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.SortOrder;
import de.otto.elasticsearch.client.aggregations.AggregationBuilder;
import de.otto.elasticsearch.client.query.QueryBuilder;
import de.otto.elasticsearch.client.response.*;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static de.otto.elasticsearch.client.response.SearchResponse.emptyResponse;
import static org.slf4j.LoggerFactory.getLogger;

public class SearchRequestBuilder implements RequestBuilder<SearchResponse> {
    private static final JsonObject EMPTY_JSON_OBJECT = new JsonObject();

    private RoundRobinLoadBalancingHttpClient httpClient;
    private final String[] indices;
    private final Gson gson;
    private String[] types;
    private JsonObject query;
    private Integer from;
    private Integer size;
    private Integer timeoutMillis;
    private JsonArray sorts;
    private JsonArray fields;
    private String scroll;
    private QueryBuilder postFilter;
    private List<AggregationBuilder> aggregations;

    public static final Logger LOG = getLogger(SearchRequestBuilder.class);

    public SearchRequestBuilder(RoundRobinLoadBalancingHttpClient httpClient, String... indices) {
        this.httpClient = httpClient;
        this.indices = indices;
        this.gson = new Gson();
    }

    public SearchRequestBuilder setScroll(String scroll) {
        this.scroll = scroll;
        return this;
    }

    public SearchRequestBuilder setTypes(String... types) {
        this.types = types;
        return this;
    }

    public SearchRequestBuilder setQuery(JsonObject query) {
        this.query = query;
        return this;
    }

    public SearchRequestBuilder addAggregation(AggregationBuilder aggregationBuilder) {
        if (aggregations == null) {
            aggregations = new ArrayList<>();
        }
        aggregations.add(aggregationBuilder);
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
        try {
            String url = RequestBuilderUtil.buildUrl(indices, types, "_search");
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
                aggregations.stream()
                        .forEach(a ->
                                jsonObject.add(a.getName(), a.build()));
                body.add("aggregations", jsonObject);
            }
            AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = httpClient
                    .preparePost(url)
                    .setBodyEncoding("UTF-8");
            if (timeoutMillis != null) {
                boundRequestBuilder.setRequestTimeout(timeoutMillis);
            }
            if (scroll != null) {
                boundRequestBuilder.addQueryParam("scroll", scroll);
            }

            Response response = boundRequestBuilder.setBody(gson.toJson(body))
                    .execute()
                    .get();

            //Did not find an entry
            if (response.getStatusCode() == 404) {
                return emptyResponse();
            }

            //Server Error
            if (response.getStatusCode() >= 300) {
                throw toHttpServerErrorException(response);
            }

            JsonObject jsonResponse = gson.fromJson(response.getResponseBody(), JsonObject.class);
            SearchResponse.Builder searchResponse = parseResponse(jsonResponse, scroll, httpClient);

            JsonElement aggregationsJsonElement = jsonResponse.get("aggregations");
            if (aggregationsJsonElement != null) {
                final JsonObject aggregationsJsonObject = aggregationsJsonElement.getAsJsonObject();

                aggregations.forEach(a -> {
                    JsonElement aggreagationElement = aggregationsJsonObject.get(a.getName());
                    if (aggreagationElement != null) {
                        Aggregation aggregation = a.parseResponse(aggreagationElement.getAsJsonObject());
                        searchResponse.addAggregation(a.getName(), aggregation);
                    }
                });
            }
            return searchResponse.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static SearchResponse.Builder parseResponse(JsonObject jsonObject, String scroll, RoundRobinLoadBalancingHttpClient client) {
        SearchResponse.Builder searchResponse = SearchResponse.builder();
        searchResponse.setTookInMillis(jsonObject.get("took").getAsLong());
        JsonObject hits = jsonObject.get("hits").getAsJsonObject();
        long totalHits = hits.get("total").getAsLong();
        JsonElement max_score = hits.get("max_score");
        Float maxScore = max_score.isJsonPrimitive() ? max_score.getAsFloat() : null;
        JsonElement scroll_id = jsonObject.get("_scroll_id");
        if (scroll_id != null) {
            searchResponse.setScrollId(scroll_id.getAsString());
        }
        JsonArray hitsArray = hits.get("hits").getAsJsonArray();

        List<SearchHit> searchHitsCurrentPage = new ArrayList<>();
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
            searchHitsCurrentPage.add(hit);
        }
        if(scroll!=null) {
            searchResponse.setHits(new ScrollingSearchHits(totalHits, maxScore, scroll_id.getAsString(), scroll, searchHitsCurrentPage, client));
        } else {
            searchResponse.setHits(new SimpleSearchHits(totalHits, maxScore, searchHitsCurrentPage));
        }
        return searchResponse;
    }

    public SearchRequestBuilder setPostFilter(QueryBuilder postFilter) {
        this.postFilter = postFilter;
        return this;
    }
}
