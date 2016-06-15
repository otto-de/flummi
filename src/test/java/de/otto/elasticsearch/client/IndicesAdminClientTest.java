package de.otto.elasticsearch.client;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.request.CreateIndexRequestBuilder;
import de.otto.elasticsearch.client.request.DeleteIndexRequestBuilder;
import de.otto.elasticsearch.client.request.IndicesExistsRequestBuilder;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class IndicesAdminClientTest {
    private IndicesAdminClient indicesAdminClient;
    private RoundRobinLoadBalancingHttpClient httpClient;

    @BeforeMethod
    public void setup() {
        httpClient = mock(RoundRobinLoadBalancingHttpClient.class);
        indicesAdminClient = new IndicesAdminClient(httpClient);
    }

    @Test
    public void shouldPrepareCreate() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePut(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"acknowledged\":\"true\"}");
        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate("someIndexName");

        //When
        createIndexRequestBuilder.execute();

        //Then
        verify(httpClient).preparePut("/someIndexName");
    }

    @Test
    public void shouldPrepareExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.prepareHead(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final IndicesExistsRequestBuilder indicesExistsRequestBuilder = indicesAdminClient.prepareExists("someIndexName");

        //When
        indicesExistsRequestBuilder.execute();

        //Then
        verify(httpClient).prepareHead("/someIndexName");
    }

    @Test
    public void shouldPrepareDelete() throws ExecutionException, InterruptedException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.prepareDelete(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final DeleteIndexRequestBuilder deleteIndexRequestBuilder = indicesAdminClient.prepareDelete("someIndexName");

        //When
        deleteIndexRequestBuilder.execute();

        //Then
        verify(httpClient).prepareDelete("/someIndexName");
    }

}