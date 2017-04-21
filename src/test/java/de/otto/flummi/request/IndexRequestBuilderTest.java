package de.otto.flummi.request;

import com.ning.http.client.AsyncHttpClient;
import de.otto.flummi.CompletedFuture;
import de.otto.flummi.MockResponse;
import de.otto.flummi.response.HttpServerErrorException;
 import org.elasticsearch.client.RestClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class IndexRequestBuilderTest {

    IndexRequestBuilder testee;

    RestClient httpClient;

    AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;

    @BeforeMethod
    public void setUp() throws Exception {
        httpClient = mock(RestClient.class);
        boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        testee = new IndexRequestBuilder(httpClient);
    }

    @Test
    public void shouldFireIndexRequestWithId() throws Exception {
        when(httpClient.preparePut(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "OK", "{\"allet tutti\":\"wa\"}")));
        testee
                .setSource(object("some", object("friggin", "source")))
                .setIndexName("someIndex")
                .setDocumentType("someType")
                .setId(4711);

        // when
        testee.execute();

        // then
        verify(httpClient).preparePut("/someIndex/someType/4711");
        verify(boundRequestBuilder).setBody("{\"some\":{\"friggin\":\"source\"}}");
    }


    @Test
    public void shouldFireIndexRequestWithoutId() throws Exception {
        when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "OK", "{\"allet tutti\":\"wa\"}")));
        testee
                .setSource(object("some", object("friggin", "source")))
                .setIndexName("someIndex")
                .setDocumentType("someType");

        // when
        testee.execute();

        // then
        verify(httpClient).preparePost("/someIndex/someType");
        verify(boundRequestBuilder).setBody("{\"some\":{\"friggin\":\"source\"}}");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowWhenServerReturnsBadStatusCode() throws Exception {
        when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture<>(new MockResponse(400, "Bad Request", "{\"query\":\"war kaputt\"}")));
        testee
                .setSource(object("some", object("friggin", "source")))
                .setIndexName("someIndex")
                .setDocumentType("someType");

        // when
        try {
            testee.execute();
        }

        // then
        catch (HttpServerErrorException e) {
            verify(httpClient).preparePost("/someIndex/someType");
            verify(boundRequestBuilder).setBody("{\"some\":{\"friggin\":\"source\"}}");
            assertThat(e.getStatusCode(), is(400));
            assertThat(e.getResponseBody(), is("{\"query\":\"war kaputt\"}"));
            throw e;
        }
    }
}
