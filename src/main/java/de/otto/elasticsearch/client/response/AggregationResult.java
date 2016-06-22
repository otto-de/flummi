package de.otto.elasticsearch.client.response;

import java.util.Map;

public class AggregationResult {
    private Map<String, AggregationResult> nestedAggregations;
    private TermsAggregation termsAggregation;

    public AggregationResult(final Map<String, AggregationResult> nestedAggregations) {
        this.nestedAggregations = nestedAggregations;
    }

    public AggregationResult(final TermsAggregation termsAggregation) {
        this.termsAggregation = termsAggregation;
    }

    public TermsAggregation getTermsAggregation() {
        return termsAggregation;
    }

    public Map<String, AggregationResult> getNestedAggregations() {
        return nestedAggregations;
    }

    public boolean hasNestedAggregation() {
        return nestedAggregations != null;
    }
}
