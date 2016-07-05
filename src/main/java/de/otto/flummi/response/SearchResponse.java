package de.otto.flummi.response;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class SearchResponse {

    private final long tookInMillis;
    private final String scrollId;
    private final SearchHits hits;
    private final Map<String,AggregationResult> aggregations;


    public SearchResponse(long tookInMillis, String scrollId, SearchHits hits, Map<String, AggregationResult> aggregations) {
        this.tookInMillis = tookInMillis;
        this.scrollId = scrollId;
        this.hits = hits;
        this.aggregations = aggregations;
    }

    public SearchHits getHits() {
        return hits;
    }

    public Map<String,AggregationResult> getAggregations() {
        return aggregations;
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    public String getScrollId() {
        return scrollId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SearchResponse emptyResponse() {
        return new SearchResponse(0, null, new SimpleSearchHits(0L, 0F, emptyList()), emptyMap());
    }

    public static final class Builder {
        private long tookInMillis;
        private String scrollId;
        private SearchHits hits;
        private Map<String,AggregationResult> aggregations = new HashMap<>();

        public Builder setTookInMillis(long tookInMillis) {
            this.tookInMillis = tookInMillis;
            return this;
        }

        public Builder setScrollId(String scrollId) {
            this.scrollId = scrollId;
            return this;
        }

        public Builder setHits(SearchHits hits) {
            this.hits = hits;
            return this;
        }

        public Builder addAggregation(String name, AggregationResult value) {
            this.aggregations.put(name, value);
            return this;
        }

        public SearchResponse build() {
            return new SearchResponse(tookInMillis, scrollId, hits, aggregations);
        }
    }
}
