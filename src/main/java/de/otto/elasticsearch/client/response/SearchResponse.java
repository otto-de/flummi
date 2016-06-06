package de.otto.elasticsearch.client.response;


import java.util.Map;

public class SearchResponse {

    private final long tookInMillis;
    private final SearchHits hits;
    private final Map<String,Aggregation> aggregations;


    public SearchResponse(long tookInMillis, SearchHits hits, Map<String,Aggregation> aggregations) {
        this.tookInMillis = tookInMillis;
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
}
