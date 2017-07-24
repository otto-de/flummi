package de.otto.flummi.response;


import java.util.Collections;
import java.util.Map;

public class Bucket {
    private final String key;
    private final Long docCount;
    private final Map<String, AggregationResult> aggregations;

    public Bucket(String key, Long docCount) {
        this(key, docCount, Collections.emptyMap());
    }

    public Bucket(String key, Long docCount, Map<String, AggregationResult> aggregations) {
        this.key = key;
        this.docCount = docCount;
        this.aggregations = aggregations;
    }

    public String getKey() {
        return key;
    }

    public Long getDocCount() {
        return docCount;
    }

    public Map<String, AggregationResult> getAggregations() {
        return aggregations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Bucket bucket = (Bucket) o;

        if (key != null ? !key.equals(bucket.key) : bucket.key != null) return false;
        if (docCount != null ? !docCount.equals(bucket.docCount) : bucket.docCount != null) return false;
        return aggregations != null ? aggregations.equals(bucket.aggregations) : bucket.aggregations == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (docCount != null ? docCount.hashCode() : 0);
        result = 31 * result + (aggregations != null ? aggregations.hashCode() : 0);
        return result;
    }
}
