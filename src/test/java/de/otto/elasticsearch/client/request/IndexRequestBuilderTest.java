package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.CompletedFuture;
import de.otto.elasticsearch.client.MockResponse;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class IndexRequestBuilderTest {

    IndexRequestBuilder testee;

    AsyncHttpClient asyncHttpClient;

    AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;
    private ImmutableList<String> hosts = ImmutableList.of("someHost:9200");


    @BeforeMethod
    public void setUp() throws Exception {
        asyncHttpClient = mock(AsyncHttpClient.class);
        boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        testee = new IndexRequestBuilder(asyncHttpClient, hosts, 0);
    }

    @Test
    public void shouldFireIndexRequestWithId() throws Exception {
        when(asyncHttpClient.preparePut(any(String.class))).thenReturn(boundRequestBuilder);
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
        verify(asyncHttpClient).preparePut("http://someHost:9200/someIndex/someType/4711");
        verify(boundRequestBuilder).setBody("{\"some\":{\"friggin\":\"source\"}}");
    }


    @Test
    public void shouldFireIndexRequestWithoutId() throws Exception {
        when(asyncHttpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "OK", "{\"allet tutti\":\"wa\"}")));
        testee
                .setSource(object("some", object("friggin", "source")))
                .setIndexName("someIndex")
                .setDocumentType("someType");

        // when
        testee.execute();

        // then
        verify(asyncHttpClient).preparePost("http://someHost:9200/someIndex/someType");
        verify(boundRequestBuilder).setBody("{\"some\":{\"friggin\":\"source\"}}");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowWhenServerReturnsBadStatusCode() throws Exception {
        when(asyncHttpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
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
            verify(asyncHttpClient).preparePost("http://someHost:9200/someIndex/someType");
            verify(boundRequestBuilder).setBody("{\"some\":{\"friggin\":\"source\"}}");
            assertThat(e.getStatusCode(), is(400));
            assertThat(e.getMessage(), is("400 Bad Request"));
            assertThat(e.getResponseBody(), is("{\"query\":\"war kaputt\"}"));
            throw e;
        }
    }
}
