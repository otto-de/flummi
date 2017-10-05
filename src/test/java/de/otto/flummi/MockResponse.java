package de.otto.flummi;

import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.Response;
import org.asynchttpclient.cookie.Cookie;
import org.asynchttpclient.uri.Uri;

import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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
    public byte[] getResponseBodyAsBytes() {
        return responseBody.getBytes();
    }

    @Override
    public ByteBuffer getResponseBodyAsByteBuffer() {
        throw new IllegalStateException("ByteBuffer not implemented");
    }

    @Override
    public InputStream getResponseBodyAsStream() {
        throw new IllegalStateException("Stream not implemented");
    }

    @Override
    public String getResponseBody() {
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

	@Override
	public String getResponseBody(Charset charset) {
		return null;
	}

	@Override
	public HttpHeaders getHeaders() {
		return null;
	}

	@Override
	public SocketAddress getRemoteAddress() {
		return null;
	}

	@Override
	public SocketAddress getLocalAddress() {
		return null;
	}
}
