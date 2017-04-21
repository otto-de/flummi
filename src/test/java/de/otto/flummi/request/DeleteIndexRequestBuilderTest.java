package de.otto.flummi.request;

import com.ning.http.client.AsyncHttpClient;
import de.otto.flummi.CompletedFuture;
import de.otto.flummi.InvalidElasticsearchResponseException;
import de.otto.flummi.MockResponse;
import de.otto.flummi.response.HttpServerErrorException;
 import org.elasticsearch.client.RestClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class DeleteIndexRequestBuilderTest {

    private RestClient httpClient;
    private DeleteIndexRequestBuilder testee;

    @BeforeMethod
    private void setup() {
        httpClient = mock(RestClient.class);
    }

    @Test
    public void shouldDeleteIndex() {
        testee = new DeleteIndexRequestBuilder(httpClient, Stream.of("someIndexName"));
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(httpClient.prepareDelete("/someIndexName")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "")));
        testee.execute();
        verify(httpClient).prepareDelete("/someIndexName");
    }

    @Test
    public void shouldDeleteMultipleIndices() throws Exception {
        testee = new DeleteIndexRequestBuilder(httpClient, Stream.of("someIndexName", "someOtherIndex"));
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(httpClient.prepareDelete("/someIndexName,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "")));
        testee.execute();
        verify(httpClient).prepareDelete("/someIndexName,someOtherIndex");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowExceptionIfStatusCodeNotOk() {
        testee = new DeleteIndexRequestBuilder(httpClient, Stream.of("someIndexName"));
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.prepareDelete("/someIndexName")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(400, "not ok", "")));
        try {
            testee.execute();
        } catch (HttpServerErrorException e) {
            assertThat(e.getStatusCode(), is(400));
            assertThat(e.getResponseBody(), is(""));
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

    @Test(expectedExceptions = RuntimeException.class)
    public void shouldNotInvokeHttpClientWhenNoIndicesAreGivenForDeletion() throws Exception {
        testee = new DeleteIndexRequestBuilder(httpClient, Stream.of());
        testee.execute();
        verifyZeroInteractions(httpClient);
    }
}