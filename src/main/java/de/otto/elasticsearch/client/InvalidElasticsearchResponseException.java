package de.otto.elasticsearch.client;

public class InvalidElasticsearchResponseException extends RuntimeException {
    public InvalidElasticsearchResponseException(String message) {
        super(message);
    }
}
