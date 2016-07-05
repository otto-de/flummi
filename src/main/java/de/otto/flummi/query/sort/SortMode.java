package de.otto.flummi.query.sort;

public enum SortMode {
    MIN("min"),
    AVG("avg"),
    MAX("max"),
    SUM("sum"),
    MEDIAN("median");

    private final String key;

    SortMode(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
