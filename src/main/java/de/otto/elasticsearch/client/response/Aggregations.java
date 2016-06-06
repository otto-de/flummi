package de.otto.elasticsearch.client.response;


import java.util.stream.Stream;

public class Aggregations {
    public <A extends Aggregation> A get(String name) {
        return null;
    }

    public Stream<Aggregation> stream() {
        return null;
    }
}
