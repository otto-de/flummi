package de.otto.flummi.util;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

public class HttpClientWrapper {

    private final AsyncHttpClient asyncHttpClient;
    private final String baseUrl;

    public HttpClientWrapper(AsyncHttpClient asyncHttpClient, String baseUrl) {
        this.asyncHttpClient = asyncHttpClient;
        this.baseUrl = baseUrl;
    }

    public BoundRequestBuilder prepareGet(String url) {
        return asyncHttpClient.prepareGet(baseUrl + url);
    }

    public BoundRequestBuilder preparePost(String url) {
        return asyncHttpClient.preparePost(baseUrl + url);
    }

    public BoundRequestBuilder preparePut(String url) {
        return asyncHttpClient.preparePut(baseUrl + url);
    }

    public BoundRequestBuilder prepareDelete(String url) {
        return asyncHttpClient.prepareDelete(baseUrl + url);
    }

    public BoundRequestBuilder prepareHead(String url) {
        return asyncHttpClient.prepareHead(baseUrl + url);
    }
}
