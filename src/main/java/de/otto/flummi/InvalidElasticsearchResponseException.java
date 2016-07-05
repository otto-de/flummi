package de.otto.flummi;

public class InvalidElasticsearchResponseException extends RuntimeException {
    public InvalidElasticsearchResponseException(String message) {
        super(message);
    }
}
