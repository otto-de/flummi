package de.otto.elasticsearch.client.response;

public class SearchHitField {
    private Object value;

    public <V> V getValue() {
        return (V) value;
    }
}
