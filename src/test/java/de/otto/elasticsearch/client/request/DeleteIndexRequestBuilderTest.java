package de.otto.elasticsearch.client.request;

import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.CompletedFuture;
import de.otto.elasticsearch.client.InvalidElasticsearchResponseException;
import de.otto.elasticsearch.client.MockResponse;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import de.otto.elasticsearch.client.util.HttpClientWrapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class DeleteIndexRequestBuilderTest {

    private HttpClientWrapper httpClient;
    private DeleteIndexRequestBuilder testee;

    @BeforeMethod
    private void setup() {
        httpClient = mock(HttpClientWrapper.class);
    }

    @Test
    public void shouldDeleteIndex() {
        testee = new DeleteIndexRequestBuilder(httpClient, "someIndexName");
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(httpClient.prepareDelete("/someIndexName")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "")));
        testee.execute();
        verify(httpClient).prepareDelete("/someIndexName");
    }

    @Test
    public void shouldDeleteMultipleIndices() throws Exception {
        testee = new DeleteIndexRequestBuilder(httpClient, "someIndexName", "someOtherIndex");
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(httpClient.prepareDelete("/someIndexName,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "")));
        testee.execute();
        verify(httpClient).prepareDelete("/someIndexName,someOtherIndex");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowExceptionIfStatusCodeNotOk() {
        testee = new DeleteIndexRequestBuilder(httpClient, "someIndexName");
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.prepareDelete("/someIndexName")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(400, "not ok", "")));
        try {
            testee.execute();
        } catch (HttpServerErrorException e) {
            assertThat(e.getMessage(), is("400 not ok"));
            throw e;
        }
    }

    @Test
    public void shouldNotDeleteIndexForAcknowledgedFalse() throws ExecutionException, InterruptedException, IOException {
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.prepareDelete("/someIndexName")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"acknowledged\":\"false\"}")));
        try {
            testee.execute();
        } catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("{\"acknowledged\":\"false\"}"));
            throw e;
        }
    }
}