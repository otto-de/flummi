package de.otto.elasticsearch.client;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.request.ClusterHealthRequestBuilder;
import de.otto.elasticsearch.client.request.CreateIndexRequestBuilder;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class AdminClientTest {

    private AdminClient adminClient;
    private RoundRobinLoadBalancingHttpClient httpClient;


    @BeforeMethod
    public void setup() {
        httpClient = mock(RoundRobinLoadBalancingHttpClient.class);
        adminClient = new AdminClient(httpClient);
    }

    @Test
    public void shouldCreateClusterAdminClient() throws ExecutionException, InterruptedException, IOException {
        // Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        final Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"status\":\"GREEN\", \"cluster_name\":\"someClusterName\", \"timed_out\":\"someTimedOut\"}");
        when(listenableFuture.get()).thenReturn(response);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
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
        final IndicesAdminClient indicesAdminClient = adminClient.indices();
        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate("someIndexName");

        //When
        createIndexRequestBuilder.execute();

        //Then
        verify(httpClient).preparePut("/someIndexName");
        assertThat(indicesAdminClient, notNullValue());
    }

}