package de.otto.flummi.util;

import com.ning.http.client.AsyncHttpClient;

public class HttpClientWrapper {

    private final AsyncHttpClient asyncHttpClient;
    private final String baseUrl;

    public HttpClientWrapper(AsyncHttpClient asyncHttpClient, String baseUrl) {
        this.asyncHttpClient = asyncHttpClient;
        this.baseUrl = baseUrl;
    }

    public AsyncHttpClient.BoundRequestBuilder prepareGet(String url) {
        return asyncHttpClient.prepareGet(baseUrl + url);
    }

    public AsyncHttpClient.BoundRequestBuilder preparePost(String url) {
        return asyncHttpClient.preparePost(baseUrl + url);
    }

    public AsyncHttpClient.BoundRequestBuilder preparePut(String url) {
        return asyncHttpClient.preparePut(baseUrl + url);
    }

    public AsyncHttpClient.BoundRequestBuilder prepareDelete(String url) {
        return asyncHttpClient.prepareDelete(baseUrl + url);
    }

    public AsyncHttpClient.BoundRequestBuilder prepareHead(String url) {
        return asyncHttpClient.prepareHead(baseUrl + url);
    }
}
