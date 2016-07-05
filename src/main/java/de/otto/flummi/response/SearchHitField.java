package de.otto.flummi.response;

public class SearchHitField {
    private Object value;

    public <V> V getValue() {
        return (V) value;
    }
}
