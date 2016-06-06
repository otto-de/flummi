package de.otto.elasticsearch.client.response;

import java.util.List;

public class TermsAggregation {
    private List<Bucket> buckets;

    public TermsAggregation(List<Bucket> buckets) {
        this.buckets = buckets;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }
}
