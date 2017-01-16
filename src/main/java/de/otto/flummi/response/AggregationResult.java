package de.otto.flummi.response;

import java.util.List;
import java.util.Map;

public interface AggregationResult {

    List<Bucket> getBuckets();

    Map<String, AggregationResult> getNestedAggregations();

    boolean hasNestedAggregation();
 }
