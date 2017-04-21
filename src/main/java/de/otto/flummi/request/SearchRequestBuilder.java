package de.otto.flummi.request;

import com.google.gson.*;
import de.otto.flummi.RequestBuilderUtil;
import de.otto.flummi.SortOrder;
import de.otto.flummi.aggregations.AggregationBuilder;
import de.otto.flummi.query.QueryBuilder;
import de.otto.flummi.query.sort.FieldSortBuilder;
import de.otto.flummi.query.sort.SortBuilder;
import de.otto.flummi.response.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collector;

import static de.otto.flummi.RequestBuilderUtil.toHttpServerErrorException;
import static de.otto.flummi.response.SearchResponse.emptyResponse;
import static org.slf4j.LoggerFactory.getLogger;

public class SearchRequestBuilder implements RequestBuilder<SearchResponse> {
    private static final JsonObject EMPTY_JSON_OBJECT = new JsonObject();

    private RestClient restClient;
    private final String[] indices;
    private final Gson gson;
    private String[] types;
    private JsonObject query;
    private Integer from;
    private Integer size;
    private Integer timeoutMillis;
    private JsonArray sorts;
    private JsonArray storedFields;
    private JsonArray sourceFilters;
    private String scroll;
    private QueryBuilder postFilter;
    private List<AggregationBuilder> aggregations;

    public static final Logger LOG = getLogger(SearchRequestBuilder.class);

    public SearchRequestBuilder(RestClient restClient, String... indices) {
        this.restClient = restClient;
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

    public SearchRequestBuilder addAggregation(AggregationBuilder AggregationBuilder) {
        if (aggregations == null) {
            aggregations = new ArrayList<>();
        }
        aggregations.add(AggregationBuilder);
        return this;
    }

    public SearchRequestBuilder addSort(String key, SortOrder order) {
        return this.addSort(new FieldSortBuilder(key).setOrder(order));
    }

    public SearchRequestBuilder addSort(SortBuilder builder) {
        if (sorts == null) {
            sorts = new JsonArray();
        }
        sorts.add(builder.build());
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

    public SearchRequestBuilder addStoredField(String fieldName) {
        if (storedFields == null) {
            storedFields = new JsonArray();
        }
        storedFields.add(new JsonPrimitive(fieldName));
        return this;
    }

    public SearchRequestBuilder addSourceFilter(String filter) {
        if (sourceFilters == null) {
            sourceFilters = new JsonArray();
        }
        sourceFilters.add(new JsonPrimitive(filter));
        return this;
    }

    public SearchRequestBuilder setTimeoutMillis(Integer timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    @Override
    public SearchResponse execute() {
        JsonObject body = new JsonObject();
        try {
            String url = RequestBuilderUtil.buildUrl(indices, types, "_search");
            if (query != null) {
                body.add("query", query);
            }
            if (storedFields != null) {
                body.add("stored_fields", storedFields);
            }
            if (sourceFilters != null) {
                body.add("_source", sourceFilters);
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
                JsonObject jsonObject = aggregations
                        .stream()
                        .collect(toJsonObject());

                body.add("aggregations", jsonObject);
            }
            if (timeoutMillis != null) {
                boundRequestBuilder.setRequestTimeout(timeoutMillis);
            }
            Map<String, String> params = Collections.emptyMap();
            if (scroll != null) {
                params = Collections.singletonMap("scroll", scroll);
            }
            Response response = restClient.performRequest("POST", url, params,
                    new StringEntity(gson.toJson(body), "UTF-8"));

            //Did not find an entry
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 404) {
                return emptyResponse();
            }

            //Server Error
            if (statusCode >= 300) {
                throw toHttpServerErrorException(response);
            }

            JsonObject jsonResponse = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonObject.class);
            SearchResponse.Builder searchResponse = parseResponse(jsonResponse, scroll, restClient);

            JsonElement aggregationsJsonElement = jsonResponse.get("aggregations");
            if (aggregationsJsonElement != null) {
                final JsonObject aggregationsJsonObject = aggregationsJsonElement.getAsJsonObject();

                aggregations.forEach(a -> {
                    JsonElement aggregationElement = aggregationsJsonObject.get(a.getName());
                    if (aggregationElement != null) {
                        AggregationResult aggregation = a.parseResponse(aggregationElement.getAsJsonObject());
                        searchResponse.addAggregation(a.getName(), aggregation);
                    }
                });
            }
            return searchResponse.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(body.toString(), e);
        }
    }

    private static Collector<AggregationBuilder, JsonObject, JsonObject> toJsonObject() {
        return Collector.of(JsonObject::new,
                (json, a) -> json.add(a.getName(), a.build()),
                (left, right) -> left);
    }

    public static SearchResponse.Builder parseResponse(JsonObject jsonObject, String scroll, RestClient client) {
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
        if (scroll != null && scroll_id != null) {
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
