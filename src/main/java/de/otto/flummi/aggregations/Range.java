package de.otto.flummi.aggregations;

public class Range {
    private final String key;
    private final Double from;
    private final Double to;

    public Range(String key, Double from, Double to) {
        this.key = key;
        this.from = from;
        this.to = to;
    }

    public String getKey() {
        return key;
    }

    public Double getFrom() {
        return from;
    }

    public Double getTo() {
        return to;
    }
}
