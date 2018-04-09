package de.otto.flummi;

import de.otto.flummi.request.ClusterHealthRequestBuilder;
import de.otto.flummi.request.CreateIndexRequestBuilder;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class AdminClientTest {

    private AdminClient adminClient;
    private HttpClientWrapper httpClient;


    @BeforeMethod
    public void setup() {
        httpClient = mock(HttpClientWrapper.class);
        adminClient = new AdminClient(httpClient);
    }

    @Test
    public void shouldCreateClusterAdminClient() throws ExecutionException, InterruptedException, IOException {
        // Given
        final BoundRequestBuilder boundRequestBuilder = mock(BoundRequestBuilder.class);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        final Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"status\":\"GREEN\", \"cluster_name\":\"someClusterName\", \"timed_out\":\"someTimedOut\"}");
        when(listenableFuture.get()).thenReturn(response);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(httpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        final ClusterAdminClient cluster = adminClient.cluster();
        final ClusterHealthRequestBuilder clusterHealthRequestBuilder = cluster.prepareHealth("someIndexName");

        //When
        clusterHealthRequestBuilder.execute();

        //Then
        Mockito.verify(httpClient).prepareGet("/_cluster/health/someIndexName");
        assertThat(cluster, notNullValue());
    }

    @Test
    public void shouldCreateIndicesAdminClient() throws ExecutionException, InterruptedException, IOException {
        //Given
        final BoundRequestBuilder boundRequestBuilder = mock(BoundRequestBuilder.class);
        when(httpClient.preparePut(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"acknowledged\":\"true\"}");
        final IndicesAdminClient indicesAdminClient = adminClient.indices();
        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate("someIndexName");

        //When
        createIndexRequestBuilder.execute();

        //Then
        verify(httpClient).preparePut("/someIndexName");
        assertThat(indicesAdminClient, notNullValue());
    }

}