package de.otto.flummi;

import de.otto.flummi.request.CountRequestBuilder;
import de.otto.flummi.response.HttpServerErrorException;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class CountRequestBuilderTest {

    private static final String INDEX_NAME = "product-index";
    private HttpClientWrapper httpClient;

    CountRequestBuilder testee;

    @BeforeMethod
    public void setUp() throws Exception {
        httpClient = mock(HttpClientWrapper.class);
        testee = new CountRequestBuilder(httpClient, INDEX_NAME);
    }

    @Test
    public void shouldBuildCorrectCountQueryWithoutTypes() throws Exception {
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.prepareGet("/product-index/_count")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"count\" : 42}")));
        when(boundRequestBuilderMock.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilderMock);

        // when
        long result = testee.execute();

        // then
        verify(httpClient).prepareGet("/product-index/_count");
        assertThat(result, is(42L));
    }

    @Test
    public void shouldBuildCorrectCountQueryWithTwoTypes() throws Exception {
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.prepareGet("/product-index/bla,blupp/_count")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"count\" : 42}")));
        when(boundRequestBuilderMock.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilderMock);

        // when
        long result = testee.setTypes("bla", "blupp").execute();

        // then
        verify(httpClient).prepareGet("/product-index/bla,blupp/_count");
        assertThat(result, is(42L));
    }

    @Test
    public void shouldBuildCorrectCountQueryWithOneType() throws Exception {
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.prepareGet("/product-index/bla/_count")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"count\" : 23}")));
        when(boundRequestBuilderMock.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilderMock);

        // when
        long result = testee.setTypes("bla").execute();

        // then
        verify(httpClient).prepareGet("/product-index/bla/_count");
        assertThat(result, is(23L));
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowWhenServerFails() throws Exception {
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.prepareGet("/product-index/bla/_count")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"error\" : \"miserable failure\" }")));
        when(boundRequestBuilderMock.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilderMock);

        // when
        try {
            testee.setTypes("bla").execute();
        }

        // then
        catch (HttpServerErrorException e) {
            verify(httpClient).prepareGet("/product-index/bla/_count");
            assertThat(e.getStatusCode(), is(500));
            assertThat(e.getResponseBody(), is("{\"error\" : \"miserable failure\" }"));
            throw e;
        }

    }
}