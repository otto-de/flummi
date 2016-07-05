package de.otto.flummi.response;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class AggregationResult {
    private final Map<String, AggregationResult> nestedAggregations;
    private final List<Bucket> buckets;

    public AggregationResult(final Map<String, AggregationResult> nestedAggregations) {
        this.nestedAggregations = nestedAggregations;
        this.buckets = emptyList();
    }

    public AggregationResult(final List<Bucket> buckets) {
        this.buckets = buckets;
        this.nestedAggregations = emptyMap();
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public Map<String, AggregationResult> getNestedAggregations() {
        return nestedAggregations;
    }

    public boolean hasNestedAggregation() {
        return !nestedAggregations.isEmpty();
    }
}
