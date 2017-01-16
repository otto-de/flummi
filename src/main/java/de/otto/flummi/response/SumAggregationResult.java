package de.otto.flummi.response;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class SumAggregationResult implements AggregationResult {
    private final Number value;

    public SumAggregationResult(Number value) {
        this.value = value;
    }

    public Number getValue() {
        return value;
    }

    @Override
    public List<Bucket> getBuckets() {
        return emptyList();
    }

    @Override
    public Map<String, AggregationResult> getNestedAggregations() {
        return emptyMap();
    }

    @Override
    public boolean hasNestedAggregation() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SumAggregationResult that = (SumAggregationResult) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "SumAggregationResult{" +
                "value=" + value +
                '}';
    }
}
