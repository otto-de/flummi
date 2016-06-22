package de.otto.elasticsearch.client.response;

import java.util.List;
import java.util.Map;

public class AggregationResult {
    private Map<String, AggregationResult> nestedAggregations;
    private List<Bucket> buckets;

    public AggregationResult(final Map<String, AggregationResult> nestedAggregations) {
        this.nestedAggregations = nestedAggregations;
    }

    public AggregationResult(final List<Bucket> buckets) {
        this.buckets = buckets;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public Map<String, AggregationResult> getNestedAggregations() {
        return nestedAggregations;
    }

    public boolean hasNestedAggregation() {
        return nestedAggregations != null;
    }
}
