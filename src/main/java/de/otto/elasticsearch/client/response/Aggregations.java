package de.otto.elasticsearch.client.response;


import java.util.stream.Stream;

public class Aggregations {
    public <A extends AggregationResult> A get(String name) {
        return null;
    }

    public Stream<AggregationResult> stream() {
        return null;
    }
}
