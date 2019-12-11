package de.otto.flummi.util;

import io.netty.handler.codec.http.HttpHeaderNames;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

import static java.util.Objects.nonNull;

public class HttpClientWrapper {

    private final AsyncHttpClient asyncHttpClient;
    private final String baseUrl;
    private final String username;
    private final String password;

    public HttpClientWrapper(AsyncHttpClient asyncHttpClient, String baseUrl) {
        this(asyncHttpClient, baseUrl, null, null);
    }

    public HttpClientWrapper(AsyncHttpClient asyncHttpClient, String baseUrl, String username, String password) {
        this.asyncHttpClient = asyncHttpClient;
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
    }

    public BoundRequestBuilder prepareGet(String url) {
        return withBasicAuth(asyncHttpClient.prepareGet(baseUrl + url));
    }

    public BoundRequestBuilder preparePost(String url) {
        return withBasicAuth(asyncHttpClient.preparePost(baseUrl + url));
    }

    public BoundRequestBuilder preparePut(String url) {
        return withBasicAuth(asyncHttpClient.preparePut(baseUrl + url));
    }

    public BoundRequestBuilder prepareDelete(String url) {
        return withBasicAuth(asyncHttpClient.prepareDelete(baseUrl + url));
    }

    public BoundRequestBuilder prepareHead(String url) {
        return withBasicAuth(asyncHttpClient.prepareHead(baseUrl + url));
    }

    private BoundRequestBuilder withBasicAuth(final BoundRequestBuilder boundRequestBuilder) {
        if (nonNull(username) && nonNull(password)) {
            boundRequestBuilder.addHeader(HttpHeaderNames.AUTHORIZATION, getAuthorizationHeaderValue(username, password));
        }
        return boundRequestBuilder;
    }

    private String getAuthorizationHeaderValue(final String username, final String password) {
        try {
            return "Basic " + Base64.getEncoder().encodeToString((username +":"+ password).getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }
}
