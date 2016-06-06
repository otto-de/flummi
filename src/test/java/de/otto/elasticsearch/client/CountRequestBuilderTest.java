package de.otto.elasticsearch.client;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.request.CountRequestBuilder;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class CountRequestBuilderTest {

    private static final String INDEX_NAME = "product-index";
    private AsyncHttpClient asyncHttpClient;
    private ImmutableList<String> HOSTS = ImmutableList.of("someHost:9200");

    CountRequestBuilder testee;

    @BeforeMethod
    public void setUp() throws Exception {
        asyncHttpClient = mock(AsyncHttpClient.class);
        testee = new CountRequestBuilder(asyncHttpClient, HOSTS, 0, INDEX_NAME);
    }

    @Test
    public void shouldBuildCorrectCountQueryWithoutTypes() throws Exception {
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet("http://someHost:9200/product-index/_count")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"count\" : 42}")));

        // when
        long result = testee.execute();

        // then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/product-index/_count");
        assertThat(result, is(42L));
    }

    @Test
    public void shouldBuildCorrectCountQueryWithTwoTypes() throws Exception {
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet("http://someHost:9200/product-index/bla,blupp/_count")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"count\" : 42}")));

        // when
        long result = testee.setTypes("bla", "blupp").execute();

        // then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/product-index/bla,blupp/_count");
        assertThat(result, is(42L));
    }

    @Test
    public void shouldBuildCorrectCountQueryWithOneType() throws Exception {
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet("http://someHost:9200/product-index/bla/_count")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"count\" : 23}")));
        // when
        long result = testee.setTypes("bla").execute();

        // then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/product-index/bla/_count");
        assertThat(result, is(23L));
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowWhenServerFails() throws Exception {
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet("http://someHost:9200/product-index/bla/_count")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"error\" : \"miserable failure\" }")));
        // when
        try {
            testee.setTypes("bla").execute();
        }

        // then
        catch(HttpServerErrorException e) {
            verify(asyncHttpClient).prepareGet("http://someHost:9200/product-index/bla/_count");
            assertThat(e.getStatusCode(), is(500));
            assertThat(e.getMessage(), is("500 Internal Server Error"));
            assertThat(e.getResponseBody(), is("{\"error\" : \"miserable failure\" }"));
            throw e;
        }

    }
}