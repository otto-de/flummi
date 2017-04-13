package de.otto.flummi;

import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Response;
import de.otto.flummi.bulkactions.DeleteActionBuilder;
import de.otto.flummi.request.*;
import de.otto.flummi.response.HttpServerErrorException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class FlummiTest {

    private Flummi client;
    private AsyncHttpClient asyncHttpClient;
    private AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;

    @BeforeMethod
    public void setUp() {
        boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        asyncHttpClient = mock(AsyncHttpClient.class);
        client = new Flummi(asyncHttpClient, "http://someHost:9200");
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        when(asyncHttpClient.prepareDelete(anyString())).thenReturn(boundRequestBuilder);
        when(asyncHttpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
    }

    @Test
    public void shouldGetIndexNameForAlias() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{\"aliases\": {\"someAliasName\": {}}}}")));

        //When
        final Optional<String> indexNameForAlias = client.getIndexNameForAlias("someAliasName");

        //Then
        assertThat(indexNameForAlias.isPresent(), is(true));
        assertThat(indexNameForAlias.get(), is("someIndexName"));
    }

    @Test
    public void shouldNotGetIndexNameForAliasIfAliasNotExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{\"aliases\": {\"someOtherAliasName\": {}}}}")));

        //When
        final Optional<String> indexNameForAlias = client.getIndexNameForAlias("someAliasName");

        //Then
        assertThat(indexNameForAlias.isPresent(), is(false));
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldNotGetIndexNameForAliasForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"someIndexName\":{\"aliases\": {\"someAliasName\": {}}}}")));

        //When
        client.getIndexNameForAlias("someAliasName");
    }

    @Test
    public void shouldPrepareSearch() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"took\":123, \"hits\": {\"total\": 3, \"max_score\":1, \"hits\": []}}")));
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        //When
        final SearchRequestBuilder searchRequestBuilder = client.prepareSearch("someIndexName");
        searchRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).preparePost("http://someHost:9200/someIndexName/_search");
        verify(boundRequestBuilder).setBodyEncoding("UTF-8");
        verify(boundRequestBuilder).setBody("{}");
    }

    @Test
    public void shouldPrepareCount() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"count\":201}");
        final CountRequestBuilder countRequestBuilder = client.prepareCount("someIndexName");

        //When
        countRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/someIndexName/_count");
    }

    @Test
    public void shouldPrepareBulk() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"errors\":false}");
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(new DeleteActionBuilder("someIndexName", "someId", "someType"));

        //When
        bulkRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).preparePost("http://someHost:9200/_bulk");
    }

    @Test
    public void shouldPrepareGet() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"_id\":\"46711\"}");
        final GetRequestBuilder getRequestBuilder = client.prepareGet("someIndexName", "someDocumentType", "someProductId");

        //When
        getRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/someIndexName/someDocumentType/someProductId");
    }

    @Test
    public void shouldPrepareDelete() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareDelete(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete();
        deleteRequestBuilder.setIndexName("someIndexName");
        deleteRequestBuilder.setDocumentType("someDocumentType");
        deleteRequestBuilder.setId("someId");

        //When
        deleteRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).prepareDelete("http://someHost:9200/someIndexName/someDocumentType/someId");
    }

    @Test
    public void shouldPrepareAnalyze() throws ExecutionException, InterruptedException, IOException {
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{ \"tokens\": [] }");
        when(listenableFuture.get()).thenReturn(response);
        final AnalyzeRequestBuilder analyzeRequestBuilder = client.prepareAnalyze("hello world");

        //When
        analyzeRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/_analyze");
    }

    @Test
    public void shouldPrepareIndex() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final IndexRequestBuilder indexRequestBuilder = client.prepareIndex();
        indexRequestBuilder.setSource(new JsonObject());

        //When
        indexRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).preparePost("http://someHost:9200");
    }

    @Test
    public void shouldGetAdminClientCluster() throws ExecutionException, InterruptedException, IOException {
        // Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        final Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"status\":\"GREEN\", \"cluster_name\":\"someClusterName\", \"timed_out\":\"someTimedOut\"}");
        when(listenableFuture.get()).thenReturn(response);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        final ClusterAdminClient cluster = client.admin().cluster();
        final ClusterHealthRequestBuilder clusterHealthRequestBuilder = cluster.prepareHealth("someIndexName");

        //When
        clusterHealthRequestBuilder.execute();

        //Then
        Mockito.verify(asyncHttpClient).prepareGet("http://someHost:9200/_cluster/health/someIndexName");
        assertThat(cluster, notNullValue());
    }

    @Test
    public void shouldGetAdminClientIndices() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.preparePut(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"acknowledged\":\"true\"}");
        final IndicesAdminClient indicesAdminClient = client.admin().indices();
        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate("someIndexName");

        //When
        createIndexRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).preparePut("http://someHost:9200/someIndexName");
        assertThat(indicesAdminClient, notNullValue());
    }
}