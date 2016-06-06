package de.otto.elasticsearch.client.response;

import java.util.Map;

public class Aggregation {
    private Map<String, Aggregation> nestedAggregations;
    private TermsAggregation termsAggregation;

    public Aggregation(final Map<String, Aggregation> nestedAggregations) {
        this.nestedAggregations = nestedAggregations;
    }

    public Aggregation(final TermsAggregation termsAggregation) {
        this.termsAggregation = termsAggregation;
    }

    public TermsAggregation getTermsAggregation() {
        return termsAggregation;
    }

    public Map<String, Aggregation> getNestedAggregations() {
        return nestedAggregations;
    }

    public boolean hasNestedAggregation() {
        return nestedAggregations != null;
    }
}
