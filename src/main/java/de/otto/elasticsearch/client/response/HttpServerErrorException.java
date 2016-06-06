package de.otto.elasticsearch.client.response;

public class HttpServerErrorException extends RuntimeException {

    private final int statusCode;
    private final String responseBody;

    public HttpServerErrorException(final int statusCode, final String message, String responseBody) {
        super(statusCode + " " + message);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
