package de.otto.elasticsearch.client.response;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class SearchResponse {

    private final long tookInMillis;
    private final String scrollId;
    private final SearchHits hits;
    private final Map<String,Aggregation> aggregations;


    public SearchResponse(long tookInMillis, String scrollId, SearchHits hits, Map<String, Aggregation> aggregations) {
        this.tookInMillis = tookInMillis;
        this.scrollId = scrollId;
        this.hits = hits;
        this.aggregations = aggregations;
    }

    public SearchHits getHits() {
        return hits;
    }

    public Map<String,Aggregation> getAggregations() {
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
        return new SearchResponse(0, null, new SearchHits(0L, 0F, ImmutableList.of()), ImmutableMap.of());
    }

    public static final class Builder {
        private long tookInMillis;
        private String scrollId;
        private SearchHits hits;
        private Map<String,Aggregation> aggregations = new HashMap<>();

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

        public Builder addAggregation(String name, Aggregation value) {
            this.aggregations.put(name, value);
            return this;
        }

        public SearchResponse build() {
            return new SearchResponse(tookInMillis, scrollId, hits, aggregations);
        }
    }
}
