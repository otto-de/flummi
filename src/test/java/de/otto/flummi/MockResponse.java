package de.otto.flummi;

import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import org.elasticsearch.client.Response;
import com.ning.http.client.cookie.Cookie;
import com.ning.http.client.uri.Uri;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Collections.emptyList;

public class MockResponse implements Response {

    private final int statusCode;
    private final String statusText;
    private final String responseBody;
    private final Uri uri;
    private final String contentType;

    public MockResponse(int statusCode, String statusText, String responseBody) {
        this(statusCode, statusText, responseBody, new Uri("http", null, "somehost", 80, "/some/uri", null), "text/html");
    }

    public MockResponse(int statusCode, String statusText, String responseBody, Uri uri, String contentType) {
        this.statusCode = statusCode;
        this.statusText = statusText;
        this.responseBody = responseBody;
        this.uri = uri;
        this.contentType = contentType;
    }

    @Override
    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public String getStatusText() {
        return statusText;
    }

    @Override
    public byte[] getResponseBodyAsBytes() throws IOException {
        return responseBody.getBytes();
    }

    @Override
    public ByteBuffer getResponseBodyAsByteBuffer() throws IOException {
        throw new IllegalStateException("ByteBuffer not implemented");
    }

    @Override
    public InputStream getResponseBodyAsStream() throws IOException {
        throw new IllegalStateException("Stream not implemented");
    }

    @Override
    public String getResponseBodyExcerpt(int maxLength, String charset) throws IOException {
        return responseBody.substring(0, maxLength-1);
    }

    @Override
    public String getResponseBody(String charset) throws IOException {
        return responseBody;
    }

    @Override
    public String getResponseBodyExcerpt(int maxLength) throws IOException {
        return responseBody.substring(0, maxLength-1);
    }

    @Override
    public String getResponseBody() throws IOException {
        return responseBody;
    }

    @Override
    public Uri getUri() {
        return uri;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public String getHeader(String name) {
        return null;
    }

    @Override
    public List<String> getHeaders(String name) {
        return emptyList();
    }

    @Override
    public FluentCaseInsensitiveStringsMap getHeaders() {
        return null;
    }

    @Override
    public boolean isRedirected() {
        return false;
    }

    @Override
    public List<Cookie> getCookies() {
        return emptyList();
    }

    @Override
    public boolean hasResponseStatus() {
        return true;
    }

    @Override
    public boolean hasResponseHeaders() {
        return false;
    }

    @Override
    public boolean hasResponseBody() {
        return !responseBody.isEmpty();
    }
}
